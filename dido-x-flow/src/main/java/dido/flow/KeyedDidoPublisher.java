package dido.flow;

public interface KeyedDidoPublisher<K> {

    DidoSubscription subscribe(KeyedDidoSubscriber<K> subscriber);
}
