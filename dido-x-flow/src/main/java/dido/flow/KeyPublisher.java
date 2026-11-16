package dido.flow;

public interface KeyPublisher<K> {

    default QuietlyCloseable subscribeKeys(KeyConsumer<? super K> keyConsumer) {
        return subscribeKeys(keyConsumer, SubscriptionOptions.of().all());
    }

    QuietlyCloseable subscribeKeys(KeyConsumer<? super K> keyConsumer, SubscriptionOptions options);
}
