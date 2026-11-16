package dido.flow;

/**
 * Publish Dido Data along with a key.
 *
 * @param <K> The key type.
 */
public interface KeyedDataPublisher<K> {

    default DidoSubscription subscribeKeyedData(KeyedDataConsumer<? super K> consumer) {
        return subscribeKeyedData(consumer, SubscriptionOptions.of().all());
    }

    DidoSubscription subscribeKeyedData(KeyedDataConsumer<? super K> consumer, SubscriptionOptions options);
}
