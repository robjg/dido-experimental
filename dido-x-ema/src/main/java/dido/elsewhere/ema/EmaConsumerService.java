package dido.elsewhere.ema;

import dido.data.DataSchema;
import dido.data.schema.SchemaAware;
import dido.flow.KeyConsumer;
import dido.flow.KeyPublisher;
import dido.flow.KeyedDataConsumer;
import dido.flow.QuietlyCloseable;
import dido.flow.util.LoggingKeyedDataConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

/**
 * @oddjob.description A server that provides a Consumer of LSEG Real Time Data.
 * T
 */
public class EmaConsumerService {

    private static final Logger logger = LoggerFactory.getLogger(EmaConsumerService.class);

    /**
     * @oddjob.description The name of the service as seen in Oddjob.
     * @oddjob.required No.
     */
    private String name;

    /**
     * @oddjob.description The host address in server:port format.
     * @oddjob.required No. See LSEG docs for defaults.
     */
    private String host;

    /**
     * @oddjob.description The service name to request data from.
     * @oddjob.required Yes.
     */
    private String serviceName;

    /**
     * @oddjob.description The directory where dictionary files are.
     * At the moment dictionary files must exist locally.
     * @oddjob.required No. Defaults to "."
     */
    private Path dictionaryDir;

    /**
     * @oddjob.description List of symbols.
     * More options required.
     * @oddjob.required No. Defaults to "."
     */
    private List<String> symbols;

    /**
     * @oddjob.description Required for the table.
     * @oddjob.required Yes
     */
    private DataSchema schema;

    /**
     * @oddjob.description If the schema is only a partial schema.
     * @oddjob.required No
     */
    private boolean partialSchema;

    /**
     * @oddjob.description The destination. Set automatically if
     * used in BeanBus.
     * @oddjob.required No.
     */
    private KeyedDataConsumer<String> to;

    private QuietlyCloseable close;

    public void start() {

        logger.info("Starting EmaConsumerService for host: {} and symbols: {}",
                host, symbols);

        KeyedDataConsumer<String> to = Objects.requireNonNullElseGet(this.to,
                LoggingKeyedDataConsumer::new);

        SchemaAware schemaAware = to instanceof SchemaAware ?
                (SchemaAware) to : null;

        List<String> symbols = Objects.requireNonNull(this.symbols);

        KeyPublisher<String> keyAvailability = new KeyPublisher<String>() {
            @Override
            public QuietlyCloseable subscribeKeyAvailability(KeyConsumer<? super String> keyConsumer) {
                symbols.forEach(keyConsumer::onAvailable);
                return () -> {};
            }
        };

        this.close = DidoOmmConsumer.with()
                .host(host)
                .serviceName(serviceName)
                .dictionaryDir(dictionaryDir)
                .schema(schema)
                .partialSchema(partialSchema)
                .schemaAware(schemaAware)
                .keyAvailability(keyAvailability)
                .to(to);
    }

    public void stop() {
        close.close();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public Path getDictionaryDir() {
        return dictionaryDir;
    }

    public void setDictionaryDir(Path dictionaryDir) {
        this.dictionaryDir = dictionaryDir;
    }

    public List<String> getSymbols() {
        return symbols;
    }

    public void setSymbols(List<String> symbols) {
        this.symbols = symbols;
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

    public KeyedDataConsumer<String> getTo() {
        return to;
    }

    public void setTo(KeyedDataConsumer<String> to) {
        this.to = to;
    }

    @Override
    public String toString() {
        return name == null ? getClass().getSimpleName() : name;
    }
}
