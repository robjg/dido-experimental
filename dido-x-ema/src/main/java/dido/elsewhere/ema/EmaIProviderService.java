package dido.elsewhere.ema;

import dido.flow.QuietlyCloseable;
import dido.table.DataTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class EmaIProviderService {

    private static final Logger logger = LoggerFactory.getLogger(EmaIProviderService.class);

    private String name;

    private int port;

    private QuietlyCloseable close;

    private DataTable<String> table;

    public void start() {

        close = DidoIProviderClient
                .with()
                .port(port)
                .dataTable(Objects.requireNonNull(
                        table, "Data Table Required"))
                .create();

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
