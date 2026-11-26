package dido.elsewhere.ema;

import dido.data.DataSchema;
import dido.data.schema.SchemaAware;
import dido.flow.KeyPublisher;
import dido.flow.KeyedDataConsumer;
import dido.flow.QuietlyCloseable;
import dido.flow.util.LoggingKeyedDataConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public class MarketPriceClientService {

    private static final Logger logger = LoggerFactory.getLogger(MarketPriceClientService.class);

    /**
     * @oddjob.description The name of the service as seen in Oddjob.
     * @oddjob.required No.
     */
    private String name;

    /**
     * @oddjob.description The service name to request data from.
     * @oddjob.required Yes.
     */
    private String serviceName;

    /**
     * @oddjob.description A schema or partial schema for the resultant data.
     * If not provided then one will be derived from the data.
     * @oddjob.required No
     */
    private DataSchema schema;

    /**
     * @oddjob.description If the schema is only a partial schema.
     * Fields not defined will be derived from the data.
     * @oddjob.required No
     */
    private boolean partialSchema;

    /**
     * @oddjob.description List of symbols.
     * @oddjob.required No.
     */
    private List<String> symbols;

    /**
     * @oddjob.description Publisher of symbols.
     * @oddjob.required No.
     */
    private KeyPublisher<String> keyPublisher;

    /**
     * @oddjob.description A mechanism for deriving symbol from
     * the feed.
     * @oddjob.required No.
     */
    private LegacyChainConsumerConfig legacyChain;

    private SchemaAware schemaAware;

    private Runnable started;

    /**
     * @oddjob.description The destination.
     * @oddjob.required No.
     */
    private KeyedDataConsumer<String> to;

    /**
     * @oddjob.description The consumer to register the client with.
     * @oddjob.required Yes.
     */
    private DidoOmmConsumer consumer;

    private QuietlyCloseable closeable;

    public CompletableFuture<Void> start() {

        DidoOmmConsumer consumer = Objects.requireNonNull(this.consumer,
                "A Dido Omm Consumer must be provided.");

        logger.info("Starting MarketPriceClientService for service: {}",
                serviceName);

        KeyedDataConsumer<String> to = Objects.requireNonNullElseGet(this.to,
                LoggingKeyedDataConsumer::new);

        SchemaAware schemaAware = to instanceof SchemaAware ?
                (SchemaAware) to : null;

        CompletableFuture<Void> future = new CompletableFuture<>();

        KeyPublisher<String> keyPublisher = this.keyPublisher;
        if (this.symbols != null) {
            keyPublisher = KeyPublisher.of(this.symbols);
        }

        DidoOmmConsumerClient client = MarketPriceClient.with()
                .serviceName(serviceName)
                .schema(schema)
                .partialSchema(partialSchema)
                .schemaAware(schemaAware)
                .keyPublisher(keyPublisher)
                .legacyChainConsumerConfig(legacyChain)
                .started(() -> future.complete(null))
                .to(to);

        this.closeable = consumer.register(client);

        return future;
    }

    public void stop() {
        this.closeable.close();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public DataSchema getSchema() {
        return schema;
    }

    public void setSchema(DataSchema schema) {
        this.schema = schema;
    }

    public boolean isPartialSchema() {
        return partialSchema;
    }

    public void setPartialSchema(boolean partialSchema) {
        this.partialSchema = partialSchema;
    }

    public List<String> getSymbols() {
        return symbols;
    }

    public void setSymbols(List<String> symbols) {
        this.symbols = symbols;
    }

    public KeyPublisher<String> getKeyPublisher() {
        return keyPublisher;
    }

    public void setKeyPublisher(KeyPublisher<String> keyPublisher) {
        this.keyPublisher = keyPublisher;
    }

    public LegacyChainConsumerConfig getLegacyChain() {
        return legacyChain;
    }

    public void setLegacyChain(LegacyChainConsumerConfig legacyChain) {
        this.legacyChain = legacyChain;
    }

    public SchemaAware getSchemaAware() {
        return schemaAware;
    }

    public void setSchemaAware(SchemaAware schemaAware) {
        this.schemaAware = schemaAware;
    }

    public Runnable getStarted() {
        return started;
    }

    public void setStarted(Runnable started) {
        this.started = started;
    }

    public KeyedDataConsumer<String> getTo() {
        return to;
    }

    public void setTo(KeyedDataConsumer<String> to) {
        this.to = to;
    }

    public DidoOmmConsumer getConsumer() {
        return consumer;
    }

    public void setConsumer(DidoOmmConsumerConnection consumer) {
        this.consumer = consumer;
    }

    @Override
    public String toString() {
        return name == null ? getClass().getSimpleName() : name;
    }
}
