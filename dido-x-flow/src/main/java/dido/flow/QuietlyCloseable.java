package dido.flow;

public interface QuietlyCloseable extends AutoCloseable {

    @Override
    void close();
}
