package dido.elsewhere.ema;

import com.refinitiv.ema.access.*;
import com.refinitiv.ema.rdm.DataDictionary;
import dido.data.DataSchema;
import dido.data.DidoData;
import dido.data.partial.PartialData;
import dido.data.schema.SchemaAware;
import dido.flow.KeyConsumer;
import dido.flow.KeyPublisher;
import dido.flow.KeyedDataConsumer;
import dido.flow.QuietlyCloseable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Wraps a {@link OmmConsumer} so that it may accept {@link DidoOmmConsumerClient}s.
 */
public class DidoOmmConsumerConnection
        implements OmmConsumerClient, DidoOmmConsumer, QuietlyCloseable {

    private static final Logger logger = LoggerFactory.getLogger(DidoOmmConsumerConnection.class);

    private final OmmConsumer consumer;

    private final KeyedDataConsumer<String> dataConsumer;

    /**
     * Currently using the field list, if no dictionary.
     */
    private final DidoOmmManager didoOmmManager;

    private final List<AutoCloseable> closeTasks;

    DidoOmmConsumerConnection(OmmConsumer consumer,
                              DidoOmmManager didoOmmManager,
                              KeyedDataConsumer<String> dataConsumer,
                              List<AutoCloseable> closeTasks) {
        this.consumer = consumer;
        this.didoOmmManager = didoOmmManager;
        this.dataConsumer = dataConsumer;
        this.closeTasks = closeTasks;
    }

    @Override
    public QuietlyCloseable register(DidoOmmConsumerClient client) {
        DataDictionary dictionary = Objects.requireNonNull(didoOmmManager.dictionary,
                "Dictionary not available yet.");

        return client.start(dictionary, consumer);
    }

    @Override
    public void close() {
        closeTasks.forEach(closeTask -> {
            try {
                closeTask.close();
            } catch (Exception e) {
                logger.warn("Failed closing {}", closeTask, e);
            }
        });
        consumer.uninitialize();
    }

    public static class Settings {

        private String host;

        private String serviceName;

        private Path configPath;

        private Path dictionaryDir;

        private DataSchema schema;

        private boolean partialSchema;

        private String username;

        private String password;

        private KeyPublisher<String> keyAvailability;

        private SchemaAware schemaAware;

        private LegacyChainConsumerConfig legacyChainConsumerConfig;

        private Runnable started;

        private KeyedDataConsumer<String> dataConsumer;

        public Settings host(String host) {
            this.host = host;
            return this;
        }

        public Settings serviceName(String serviceName) {
            this.serviceName = serviceName;
            return this;
        }

        public Settings configPath(Path configPath) {
            this.configPath = configPath;
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

        public Settings schemaAware(SchemaAware schemaAware) {
            this.schemaAware = schemaAware;
            return this;
        }

        public Settings legacyChainConsumerConfig(LegacyChainConsumerConfig legacyChainConsumerConfig) {
            this.legacyChainConsumerConfig = legacyChainConsumerConfig;
            return this;
        }

        public Settings keyAvailability(KeyPublisher<String> keyAvailability) {
            this.keyAvailability = keyAvailability;
            return this;
        }

        public Settings started(Runnable started) {
            this.started = started;
            return this;
        }

        public Settings to(KeyedDataConsumer<String> dataConsumer) {
            this.dataConsumer = dataConsumer;
            return this;
        }

        public DidoOmmConsumerConnection start() {

            List<AutoCloseable> closeTasks = new ArrayList<>();

            String serviceName = Objects.requireNonNullElse(this.serviceName,
                    "DIRECT_FEED");

            OmmConsumerConfig config;
            if (configPath == null) {
                logger.info("Creating default OmmConsumerConfig");
                config = EmaFactory.createOmmConsumerConfig();
            } else {
                logger.info("Creating OmmConsumerConfig from {}", configPath);
                config = EmaFactory.createOmmConsumerConfig(configPath.toString());
            }

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

            List<Consumer<? super DataDictionary>> dictionaryConsumers = new ArrayList<>();

            DidoOmmManager didoOmmManager = new DidoOmmManager(schema,
                    partialSchema, schemaAware);
            dictionaryConsumers.add(didoOmmManager::onDictionary);

            DidoOmmConsumerConnection client = new DidoOmmConsumerConnection(
                    consumer, didoOmmManager, dataConsumer, closeTasks);

            ReqMsg reqMsg = EmaFactory.createReqMsg();

            if (legacyChainConsumerConfig != null) {

                dictionaryConsumers.add(dictionary ->
                        {
                            LegacyChainConsumer legacyChainConsumer = legacyChainConsumerConfig.forDictionary(dictionary);

                            ChainConsumerClient chainConsumerClient = new ChainConsumerClient(
                                    consumer, serviceName, legacyChainConsumer);
                            chainConsumerClient.subscribeChain(legacyChainConsumer.rootChainName());
                            closeTasks.add(chainConsumerClient);

                            logger.info("Starting Key Subscription to Legacy Chain {}", legacyChainConsumer);
                            KeySubscriber keySubscriber = new KeySubscriber(consumer, serviceName, client, reqMsg);
                            closeTasks.add(legacyChainConsumer.subscribeKeys(keySubscriber));
                            closeTasks.add(keySubscriber);
                        }
                );
            }

            if (this.keyAvailability != null) {
                dictionaryConsumers.add(ignored -> {
                    logger.info("Starting Key Subscription to Key Publisher {}", keyAvailability);
                    KeySubscriber keySubscriber = new KeySubscriber(consumer, serviceName, client, reqMsg);
                    closeTasks.add(keyAvailability.subscribeKeys(keySubscriber));
                    closeTasks.add(keySubscriber);
                });
            }

            if (started != null) {
                dictionaryConsumers.add(ignored -> {
                    logger.info("Signalling Consumer Start.");
                    started.run();
                });
            }

            DataDictionary dictionary = EmaFactory.createDataDictionary();
            Consumer<DataDictionary> onDictionary = d -> dictionaryConsumers.forEach(
                    c -> c.accept(d));
            if (dictionaryDir == null) {
                logger.info("No local dictionary. Attempting to download dictionary.");
                DictionaryConsumerClient dictionaryClient = new DictionaryConsumerClient(
                        dictionary, reqMsg, onDictionary);
                dictionaryClient.register(consumer);
            } else {
                dictionary.loadFieldDictionary(dictionaryDir
                        .resolve("RDMFieldDictionary").toString());
                dictionary.loadEnumTypeDictionary(dictionaryDir
                        .resolve("enumtype.def").toString());
                onDictionary.accept(dictionary);
            }

            logger.info("Started Consumer to host {}, service name {}", host, serviceName);

            return client;
        }
    }

    static class KeySubscriber implements KeyConsumer<String>, AutoCloseable {

        private final java.util.Map<String, Long> handles = new HashMap<>();

        private final OmmConsumer consumer;

        private final String serviceName;

        private final DidoOmmConsumerConnection client;

        private final ReqMsg reqMsg;

        KeySubscriber(OmmConsumer consumer, String serviceName, DidoOmmConsumerConnection client, ReqMsg reqMsg) {
            this.consumer = consumer;
            this.serviceName = serviceName;
            this.client = client;
            this.reqMsg = reqMsg;
        }

        @Override
        public void onKey(String key) {

            logger.debug("Subscribing to {}", key);

            long handle = consumer.registerClient(reqMsg.clear()
                            .serviceName(serviceName)
                            .name(key),
                    client);

            handles.put(key, handle);
        }

        @Override
        public void onDelete(String key) {
            consumer.unregister(handles.get(key));
        }

        @Override
        public void close() {
            handles.values().forEach(consumer::unregister);
        }
    }


    // No or partial schema, we need to wait for a field
    // list at the moment. There's now way of knowing from
    // a directory what fields a service provides.
    static class DidoOmmManager {

        private final DataSchema schema;

        private final boolean partialSchema;

        private final SchemaAware schemaAware;

        private volatile Function<FieldList, DidoOmmData> didoOmmDataFunction;

        private volatile DidoOmmData didoOmmData;

        private volatile DataDictionary dictionary;

        DidoOmmManager(DataSchema schema,
                       boolean partialSchema,
                       SchemaAware schemaAware) {
            this.schema = schema;
            this.partialSchema = partialSchema;
            this.schemaAware = schemaAware;
        }

        void onDictionary(DataDictionary dictionary) {
            if (partialSchema || schema == null) {
                logger.info("Waiting for field list to define Schema.");
                didoOmmDataFunction =
                        fieldList -> {
                            DidoOmmData dod = DidoOmmData.with()
                                    .schema(schema)
                                    .partialSchema(partialSchema)
                                    .of(dictionary, fieldList);
                            logger.info("Schema now known: {}", dod.getSchema());
                            notifySchema(dod.getSchema());
                            return dod;
                        };
            } else {
                DidoOmmData didoOmmData = DidoOmmData.with()
                        .schema(schema)
                        .of(dictionary);
                didoOmmDataFunction = fieldList -> didoOmmData;
                notifySchema(didoOmmData.getSchema());
            }
            this.dictionary = dictionary;
        }

        void notifySchema(DataSchema schema) {
            if (schemaAware != null) {
                schemaAware.setSchema(schema);
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
        logger.info("onStatusMsg: Ignoring {}", statusMsg);
    }

    public void onGenericMsg(GenericMsg genericMsg, OmmConsumerEvent consumerEvent) {
        logger.info("onGenericMsg: Ignoring {}", genericMsg);
    }

    public void onAckMsg(AckMsg ackMsg, OmmConsumerEvent consumerEvent) {
        logger.info("onAckMsg: Ignoring {}", ackMsg);
    }

    public void onAllMsg(Msg msg, OmmConsumerEvent consumerEvent) {
    }

    void decode(Msg msg, boolean complete) {

        int dataType = msg.payload().dataType();

        if (dataType == DataType.DataTypes.FIELD_LIST) {

            FieldList fieldList = msg.payload().fieldList();
            if (complete) {
                if (dataConsumer != null) {
                    dataConsumer.onData(msg.name(),
                            didoOmmManager.onFieldList(fieldList));
                }
            } else {
                if (dataConsumer != null) {
                    dataConsumer.onPartial(msg.name(),
                            didoOmmManager.onPartialFieldList(fieldList));
                }
            }
        } else {
            logger.warn("Unsupported data type: {}", dataType);
        }
    }
}
