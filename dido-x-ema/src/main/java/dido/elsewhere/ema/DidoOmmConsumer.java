package dido.elsewhere.ema;

import com.refinitiv.ema.access.*;
import com.refinitiv.ema.rdm.DataDictionary;
import dido.data.DataSchema;
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

    /** Currently using the field list, if no dictionary. */
    private final Function<FieldList, DidoOmmData> didoOmmDataFunction;

    private DidoOmmData didoOmmData;

    public DidoOmmConsumer(Function<FieldList, DidoOmmData> didoOmmDataFunction,
                           KeyedDataConsumer<String> dataConsumer) {
        this.didoOmmDataFunction = didoOmmDataFunction;
        this.dataConsumer = dataConsumer;
    }

    public static class Settings {

        private String host;

        private String serviceName;

        private Path dictionaryDir;

        private DataSchema schema;

        private boolean partialSchema;

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

        public  Settings schema(DataSchema schema) {
            this.schema = schema;
            return this;
        }

        public Settings partialSchema(boolean partialSchema) {
            this.partialSchema = partialSchema;
            return this;
        }

        public Settings keyAvailability(KeyPublisher<String> keyAvailability) {
            this.keyAvailability = keyAvailability;
            return this;
        }

        public QuietlyCloseable to(KeyedDataConsumer<String> dataConsumer) {

            String serviceName = Objects.requireNonNull(this.serviceName,
                    "Service name cannot be null");

            Path dictionaryDir = Objects.requireNonNullElse(
                    this.dictionaryDir, Path.of("."));

            DataDictionary dictionary = EmaFactory.createDataDictionary();
            dictionary.loadFieldDictionary(dictionaryDir
                    .resolve("RDMFieldDictionary").toString());
            dictionary.loadEnumTypeDictionary(dictionaryDir
                    .resolve("enumtype.def").toString());

            OmmConsumerConfig config = EmaFactory.createOmmConsumerConfig();

            OmmConsumer consumer = EmaFactory.createOmmConsumer(
                    config.host(host)
                            .username("user"));

            // No or partial schema, we need to wait for a field
            // list at the moment.
            // TODO - I think we can look up the service here
            //  and get the field list.
            Function<FieldList, DidoOmmData> didoOmmDataFunction;
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

            DidoOmmConsumer client = new DidoOmmConsumer(
                    didoOmmDataFunction, dataConsumer);

            java.util.Map<String, Long> handles = new HashMap<>();

            @SuppressWarnings("resource")
            QuietlyCloseable subscription = keyAvailability.subscribeKeyAvailability(new KeyConsumer<>() {

                @Override
                public void onAvailable(String key) {
                    ReqMsg reqMsg = EmaFactory.createReqMsg();

                    long handle = consumer.registerClient(reqMsg.serviceName(serviceName)
                            .name(key), client);

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

    public static Settings with() {

        return new Settings();
    }

    void ensureDidoOmmData(FieldList fieldList) {

        if (didoOmmData == null) {
            didoOmmData = didoOmmDataFunction.apply(fieldList) ;
        }
    }

    public void onRefreshMsg(RefreshMsg refreshMsg, OmmConsumerEvent event) {

        logger.debug("onRefreshMsg: {}", refreshMsg);

        if (DataType.DataTypes.FIELD_LIST == refreshMsg.payload().dataType()) {

            FieldList fieldList = refreshMsg.payload().fieldList();
            ensureDidoOmmData(fieldList);
            dataConsumer.onData(refreshMsg.name(), didoOmmData.data(fieldList));
        } else {
            logger.warn("onRefreshMsg: unsupported data type");
        }
    }

    public void onUpdateMsg(UpdateMsg updateMsg, OmmConsumerEvent event) {

        logger.debug("onUpdateMsg: {}", updateMsg);

        if (DataType.DataTypes.FIELD_LIST == updateMsg.payload().dataType()) {

            FieldList fieldList = updateMsg.payload().fieldList();

            dataConsumer.onPartial(updateMsg.name(), didoOmmData.partial(fieldList));
        } else {
            logger.warn("onUpdateMsg: unsupported data type");
        }
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
        logger.info("onAllMsg: Assume already seen {}", msg);
    }
}
