package dido.flow;

public interface KeyPublisher<K> {

    QuietlyCloseable subscribeKeys(KeyConsumer<? super K> keyConsumer);
}
