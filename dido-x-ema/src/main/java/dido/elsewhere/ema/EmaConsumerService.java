package dido.elsewhere.ema;

import dido.data.DataSchema;
import dido.data.immutable.NonBoxedDataFactoryProvider;
import dido.flow.KeyConsumer;
import dido.flow.KeyPublisher;
import dido.flow.QuietlyCloseable;
import dido.table.internal.ConcurrentTableBasic;
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

    private String name;

    private String host;

    private String serviceName;

    private Path dictionaryDir;

    private List<String> symbols;

    private ConcurrentTableBasic<String> table;

    private DataSchema schema;

    private QuietlyCloseable close;

    public void start() {

        logger.info("Starting EmaConsumerService for host: {} and symbols: {}",
                host, symbols);

        this.table = ConcurrentTableBasic.create(schema,
                new NonBoxedDataFactoryProvider());

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
                .keyAvailability(keyAvailability)
                .to(table);

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

    public ConcurrentTableBasic<String> getTable() {
        return table;
    }

    @Override
    public String toString() {
        return name == null ? getClass().getSimpleName() : name;
    }
}
