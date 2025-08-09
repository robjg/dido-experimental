package dido.flow;

public interface QuietlyCloseable extends AutoCloseable {

    @Override
    void close();

    static QuietlyCloseable of(QuietlyCloseable... closeables) {
        return () -> {
            for (QuietlyCloseable closeable : closeables) {
                closeable.close();
            }
        };
    }
}
