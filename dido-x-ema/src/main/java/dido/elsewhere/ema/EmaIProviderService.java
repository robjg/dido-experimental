package dido.elsewhere.ema;

import dido.flow.QuietlyCloseable;
import dido.table.DataTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.Objects;

public class EmaIProviderService {

    private static final Logger logger = LoggerFactory.getLogger(EmaIProviderService.class);

    private String name;

    private int port;

    private QuietlyCloseable close;

    private DataTable<String> table;

    private Path configPath;

    private Path fieldDictionaryPath;

    private Path enumTypeDictionaryPath;

    public void start() {

        close = DidoOmmIProvider
                .with()
                .port(port)
                .configPath(configPath)
                .fieldDictionaryPath(fieldDictionaryPath)
                .enumTypeDictionaryPath(enumTypeDictionaryPath)
                .from(Objects.requireNonNull(
                        table, "Data Table Required"));
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

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public Path getConfigPath() {
        return configPath;
    }

    public void setConfigPath(Path configPath) {
        this.configPath = configPath;
    }

    public Path getFieldDictionaryPath() {
        return fieldDictionaryPath;
    }

    public void setFieldDictionaryPath(Path fieldDictionaryPath) {
        this.fieldDictionaryPath = fieldDictionaryPath;
    }

    public Path getEnumTypeDictionaryPath() {
        return enumTypeDictionaryPath;
    }

    public void setEnumTypeDictionaryPath(Path enumTypeDictionaryPath) {
        this.enumTypeDictionaryPath = enumTypeDictionaryPath;
    }

    public DataTable<String> getTable() {
        return table;
    }

    public void setTable(DataTable<String> table) {
        this.table = table;
    }

    @Override
    public String toString() {
        return name == null ? getClass().getSimpleName() : name;
    }
}
