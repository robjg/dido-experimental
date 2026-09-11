package dido.flow;

public interface KeyedDataPublisher<K> {

    DidoSubscription subscribe(KeyedDataConsumer<? super K> consumer);
}
