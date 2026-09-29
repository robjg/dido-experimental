package dido.flow;

public interface KeyPublisher<K> {

    QuietlyCloseable subscribeKeyAvailability(KeyConsumer<? super K> keyConsumer);
}
