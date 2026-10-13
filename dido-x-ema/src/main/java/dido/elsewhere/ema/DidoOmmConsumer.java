package dido.elsewhere.ema;

import com.refinitiv.ema.access.*;
import com.refinitiv.ema.rdm.DataDictionary;
import com.refinitiv.ema.rdm.EmaRdm;
import dido.data.DataSchema;
import dido.data.DidoData;
import dido.data.partial.PartialData;
import dido.flow.KeyConsumer;
import dido.flow.KeyPublisher;
import dido.flow.KeyedDataConsumer;
import dido.flow.QuietlyCloseable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Objects;
import java.util.function.Function;

public class DidoOmmConsumer implements OmmConsumerClient {

    private static final Logger logger = LoggerFactory.getLogger(DidoOmmConsumer.class);

    private final KeyedDataConsumer<String> dataConsumer;

    /**
     * Currently using the field list, if no dictionary.
     */
    private final DidoOmmManager didoOmmManager;

    private final DataDictionary dataDictionary;

    private boolean fldDictComplete = false;
    private boolean enumTypeComplete = false;

    DidoOmmConsumer(DataDictionary dataDictionary,
                    DidoOmmManager didoOmmManager,
                    KeyedDataConsumer<String> dataConsumer) {
        this.dataDictionary = dataDictionary;
        this.didoOmmManager = didoOmmManager;
        this.dataConsumer = dataConsumer;
    }

    public static class Settings {

        private String host;

        private String serviceName;

        private Path dictionaryDir;

        private DataSchema schema;

        private boolean partialSchema;

        private String username;

        private String password;

        private KeyPublisher<String> keyAvailability;

        public Settings host(String host) {
            this.host = host;
            return this;
        }

        public Settings serviceName(String serviceName) {
            this.serviceName = serviceName;
            return this;
        }

        public Settings dictionaryDir(Path dictionaryDir) {
            this.dictionaryDir = dictionaryDir;
            return this;
        }

        public Settings schema(DataSchema schema) {
            this.schema = schema;
            return this;
        }

        public Settings partialSchema(boolean partialSchema) {
            this.partialSchema = partialSchema;
            return this;
        }

        public Settings username(String username) {
            this.username = username;
            return this;
        }

        public Settings password(String password) {
            this.password = password;
            return this;
        }

        public Settings keyAvailability(KeyPublisher<String> keyAvailability) {
            this.keyAvailability = keyAvailability;
            return this;
        }

        public QuietlyCloseable to(KeyedDataConsumer<String> dataConsumer) {

            String serviceName = Objects.requireNonNull(this.serviceName,
                    "Service name cannot be null");

            OmmConsumerConfig config = EmaFactory.createOmmConsumerConfig();
            if (host != null) {
                config = config.host(host);
            }
            if (username != null) {
                config = config.username(username);
            }
            if (password != null) {
                config = config.password(password);
            }

            OmmConsumer consumer = EmaFactory.createOmmConsumer(config);

            DidoOmmManager didoOmmManager = new DidoOmmManager(schema, partialSchema);

            DataDictionary dictionary = EmaFactory.createDataDictionary();
            DidoOmmConsumer client = new DidoOmmConsumer(dictionary,
                    didoOmmManager, dataConsumer);

            ReqMsg reqMsg = EmaFactory.createReqMsg();

            if (dictionaryDir == null) {

                consumer.registerClient(reqMsg.clear().domainType(EmaRdm.MMT_DICTIONARY).name("RWFFld").
                        filter(EmaRdm.DICTIONARY_NORMAL), client);

                consumer.registerClient(reqMsg.clear().domainType(EmaRdm.MMT_DICTIONARY).name("RWFEnum").
                        filter(EmaRdm.DICTIONARY_NORMAL), client);
            } else {
                dictionary.loadFieldDictionary(dictionaryDir
                        .resolve("RDMFieldDictionary").toString());
                dictionary.loadEnumTypeDictionary(dictionaryDir
                        .resolve("enumtype.def").toString());
                didoOmmManager.onDictionary(dictionary);
            }

            java.util.Map<String, Long> handles = new HashMap<>();

            @SuppressWarnings("resource")
            QuietlyCloseable subscription = keyAvailability.subscribeKeyAvailability(new KeyConsumer<>() {

                @Override
                public void onAvailable(String key) {

                    long handle = consumer.registerClient(reqMsg.clear()
                                    .serviceName(serviceName)
                                    .name(key),
                            client);

                    handles.put(key, handle);
                }

                @Override
                public void onRemoved(String key) {
                    consumer.unregister(handles.get(key));
                }
            });

            logger.info("Started Consumer to host {}, service name {}", host, serviceName);

            return () -> {
                subscription.close();
                handles.values().forEach(consumer::unregister);
                consumer.uninitialize();
            };
        }
    }

    // No or partial schema, we need to wait for a field
    // list at the moment.
    // TODO - I think we can look up the service here
    //  and get the field list.
    static class DidoOmmManager {

        private final DataSchema schema;

        private final boolean partialSchema;

        private Function<FieldList, DidoOmmData> didoOmmDataFunction;

        private DidoOmmData didoOmmData;

        DidoOmmManager(DataSchema schema, boolean partialSchema) {
            this.schema = schema;
            this.partialSchema = partialSchema;
        }

        void onDictionary(DataDictionary dictionary) {
            if (partialSchema || schema == null) {
                didoOmmDataFunction =
                        fieldList -> DidoOmmData.with()
                                .schema(schema)
                                .partialSchema(partialSchema)
                                .of(dictionary, fieldList);
            } else {
                DidoOmmData didoOmmData = DidoOmmData.with()
                        .schema(schema)
                        .of(dictionary);
                didoOmmDataFunction = fieldList -> didoOmmData;
            }
        }


        DidoData onFieldList(FieldList fieldList) {
            if (didoOmmData == null) {
                didoOmmData = didoOmmDataFunction.apply(fieldList);
            }
            return didoOmmData.data(fieldList);
        }

        PartialData onPartialFieldList(FieldList fieldList) {
            return didoOmmData.partial(fieldList);
        }
    }

    public static Settings with() {

        return new Settings();
    }

    public void onRefreshMsg(RefreshMsg refreshMsg, OmmConsumerEvent event) {

        logger.debug("onRefreshMsg: {}", refreshMsg);

        decode(refreshMsg, refreshMsg.complete());
    }

    public void onUpdateMsg(UpdateMsg updateMsg, OmmConsumerEvent event) {

        logger.debug("onUpdateMsg: {}", updateMsg);

        decode(updateMsg, false);
    }

    public void onStatusMsg(StatusMsg statusMsg, OmmConsumerEvent event) {
        logger.info("onUpdateMsg: Ignoring {}", statusMsg);
    }

    public void onGenericMsg(GenericMsg genericMsg, OmmConsumerEvent consumerEvent) {
        logger.info("onGenricMsg: Ignoring {}", genericMsg);
    }

    public void onAckMsg(AckMsg ackMsg, OmmConsumerEvent consumerEvent) {
        logger.info("onAckMsg: Ignoring {}", ackMsg);
    }

    public void onAllMsg(Msg msg, OmmConsumerEvent consumerEvent) {
    }

    void decode(Msg msg, boolean complete) {

        int dataType = msg.payload().dataType();

        switch (dataType) {
            case DataType.DataTypes.SERIES:

                if (msg.name().equals("RWFFld")) {
                    dataDictionary.decodeFieldDictionary(msg.payload().series(), EmaRdm.DICTIONARY_NORMAL);

                    if (complete) {
                        fldDictComplete = true;
                    }
                } else if (msg.name().equals("RWFEnum")) {
                    dataDictionary.decodeEnumTypeDictionary(msg.payload().series(), EmaRdm.DICTIONARY_NORMAL);

                    if (complete) {
                        enumTypeComplete = true;
                    }
                }

                if (fldDictComplete && enumTypeComplete) {
                    didoOmmManager.onDictionary(dataDictionary);
                }

                break;
            case DataType.DataTypes.FIELD_LIST:

                FieldList fieldList = msg.payload().fieldList();
                if (complete) {
                    dataConsumer.onData(msg.name(),
                            didoOmmManager.onFieldList(fieldList));
                } else {
                    dataConsumer.onPartial(msg.name(),
                            didoOmmManager.onPartialFieldList(fieldList));
                }
                break;
            default:
                logger.warn("Unsupported data type: {}", dataType);
        }
    }
}
