package dido.table;

public interface QuietlyCloseable extends AutoCloseable {

    @Override
    void close();
}
