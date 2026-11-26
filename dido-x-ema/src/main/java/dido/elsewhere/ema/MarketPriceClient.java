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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

public class MarketPriceClient implements OmmConsumerClient {

    private static final Logger logger = LoggerFactory.getLogger(MarketPriceClient.class);

    private final KeyedDataConsumer<? super String> dataConsumer;

    private final KeyTrigger keyTrigger;

    /**
     * Currently using the field list, if no dictionary.
     */
    private final DidoOmmManager didoOmmManager;

    MarketPriceClient(DidoOmmManager didoOmmManager,
                      KeyedDataConsumer<? super String> dataConsumer,
                      KeyTrigger keyTrigger) {
        this.didoOmmManager = didoOmmManager;
        this.dataConsumer = dataConsumer;
        this.keyTrigger = keyTrigger;
    }

    public static class Settings implements DidoOmmConsumerClient {

        private String serviceName;

        private DataSchema schema;

        private boolean partialSchema;

        private KeyPublisher<String> keyPublisher;

        private LegacyChainConsumerConfig legacyChainConsumerConfig;

        private SchemaAware schemaAware;

        private Runnable started;

        private KeyedDataConsumer<? super String> dataConsumer;

        public Settings serviceName(String serviceName) {
            this.serviceName = serviceName;
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

        public Settings schemaAware(SchemaAware schemaAware) {
            this.schemaAware = schemaAware;
            return this;
        }

        public Settings keyPublisher(KeyPublisher<String> keyPublisher) {
            this.keyPublisher = keyPublisher;
            return this;
        }

        public Settings legacyChainConsumerConfig(LegacyChainConsumerConfig legacyChainConsumerConfig) {
            this.legacyChainConsumerConfig = legacyChainConsumerConfig;
            return this;
        }

        public Settings started(Runnable started) {
            this.started = started;
            return this;
        }

        public Settings to(KeyedDataConsumer<? super String> dataConsumer) {
            this.dataConsumer = dataConsumer;
            return this;
        }

        @Override
        public QuietlyCloseable start(DataDictionary dictionary,
                                      OmmConsumer consumer) {

            List<AutoCloseable> closeTasks = new ArrayList<>();

            String serviceName = Objects.requireNonNull(this.serviceName,
                    "Service name cannot be null");

            DidoOmmManager didoOmmManager = didoOmmManager(dictionary,
                    schema, partialSchema, schemaAware);

            KeyTrigger keyTrigger = new KeyTrigger(Objects.requireNonNullElse(
                    this.started, () -> {
                    }));

            MarketPriceClient client = new MarketPriceClient(didoOmmManager,
                    Objects.requireNonNull(dataConsumer, "No Data Destination"),
                    keyTrigger);

            ReqMsg reqMsg = EmaFactory.createReqMsg();

            if (legacyChainConsumerConfig != null) {

                LegacyChainConsumer legacyChainConsumer = legacyChainConsumerConfig.forDictionary(dictionary);

                ChainConsumerClient chainConsumerClient = new ChainConsumerClient(
                        consumer, serviceName, legacyChainConsumer);
                chainConsumerClient.subscribeChain(legacyChainConsumer.rootChainName());
                closeTasks.add(chainConsumerClient);

                logger.info("Starting Key Subscription to Legacy Chain {}", legacyChainConsumer);
                KeySubscriber keySubscriber = new KeySubscriber(
                        consumer, serviceName, client, reqMsg, keyTrigger);
                closeTasks.add(legacyChainConsumer.subscribeKeys(keySubscriber));
                closeTasks.add(keySubscriber);
            }

            if (this.keyPublisher != null) {
                logger.info("Starting Key Subscription to Key Publisher {}", keyPublisher);
                KeySubscriber keySubscriber = new KeySubscriber(consumer,
                        serviceName, client, reqMsg, keyTrigger);
                closeTasks.add(keyPublisher.subscribeKeys(keySubscriber));
                closeTasks.add(keySubscriber);
            }

            logger.info("Started Client to service name {}", serviceName);

            return () -> {
                closeTasks.forEach(closeTask -> {
                    try {
                        closeTask.close();
                    } catch (Exception e) {
                        logger.warn("Failed closing {}", closeTask, e);
                    }
                });
            };
        }
    }

    static class KeyTrigger {

        private final Runnable trigger;

        private final AtomicInteger keys = new AtomicInteger();

        private volatile boolean cocked;

        KeyTrigger(Runnable trigger) {
            this.trigger = trigger;
        }

        void add(String key) {
            if (!cocked) {
                keys.incrementAndGet();
            }
        }

        void cock() {
            logger.info("Keys known. Now waiting for Refresh for {} remaining keys.",
                    keys.get());
            if (keys.get() <= 0) {
                trigger();
            } else {
                this.cocked = true;
            }
        }

        void remove(String key) {
            int count = keys.decrementAndGet();
            if (cocked && count == 0) {
                trigger();
            }
        }

        void trigger() {
            logger.info("Signalling Market Price Client start.");
            trigger.run();
            cocked = false;
        }
    }

    static class KeySubscriber implements KeyConsumer<String>, AutoCloseable {

        private final java.util.Map<String, Long> handles = new HashMap<>();

        private final OmmConsumer consumer;

        private final String serviceName;

        private final MarketPriceClient client;

        private final ReqMsg reqMsg;

        private final KeyTrigger keyTrigger;

        KeySubscriber(OmmConsumer consumer,
                      String serviceName,
                      MarketPriceClient client,
                      ReqMsg reqMsg,
                      KeyTrigger keyTrigger) {
            this.consumer = consumer;
            this.serviceName = serviceName;
            this.client = client;
            this.reqMsg = reqMsg;
            this.keyTrigger = keyTrigger;
        }

        @Override
        public void snapshotComplete() {
            keyTrigger.cock();
        }

        @Override
        public void onKey(String key) {

            logger.debug("Subscribing to {}", key);
            keyTrigger.add(key);
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

    static DidoOmmManager didoOmmManager(DataDictionary dictionary,
                                         DataSchema schema,
                                         boolean partialSchema,
                                         SchemaAware schemaAware) {

        Consumer<DataSchema> schemaConsumer = schemaAware == null ?
                ignored -> {
                } : schemaAware::setSchema;

        Function<FieldList, DidoOmmData> didoOmmDataFunction;

        if (partialSchema || schema == null) {
            logger.info("Waiting for field list to define Schema.");
            didoOmmDataFunction =
                    fieldList -> {
                        DidoOmmData dod = DidoOmmData.with()
                                .schema(schema)
                                .partialSchema(partialSchema)
                                .of(dictionary, fieldList);
                        logger.info("Schema now known: {}", dod.getSchema());
                        schemaConsumer.accept(dod.getSchema());
                        return dod;
                    };
        } else {
            DidoOmmData didoOmmData = DidoOmmData.with()
                    .schema(schema)
                    .of(dictionary);
            didoOmmDataFunction = fieldList -> didoOmmData;
            schemaConsumer.accept(didoOmmData.getSchema());
        }

        return new DidoOmmManager(didoOmmDataFunction);
    }

    // No or partial schema, we need to wait for a field
    // list at the moment. There's now way of knowing from
    // a directory what fields a service provides.
    static class DidoOmmManager {

        private final Function<FieldList, DidoOmmData> didoOmmDataFunction;

        private volatile DidoOmmData didoOmmData;

        DidoOmmManager(Function<FieldList, DidoOmmData> didoOmmDataFunction) {
            this.didoOmmDataFunction = didoOmmDataFunction;
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
                dataConsumer.onData(msg.name(),
                        didoOmmManager.onFieldList(fieldList));
                keyTrigger.remove(msg.name());
            } else {
                dataConsumer.onPartial(msg.name(),
                        didoOmmManager.onPartialFieldList(fieldList));
            }
        } else {
            logger.warn("Unsupported data type: {}", dataType);
        }
    }
}
