package dido.flow;

import java.util.List;

public interface KeyPublisher<K> {

    default QuietlyCloseable subscribeKeys(KeyConsumer<? super K> keyConsumer) {
        return subscribeKeys(keyConsumer, SubscriptionOptions.of().all());
    }

    QuietlyCloseable subscribeKeys(KeyConsumer<? super K> keyConsumer, SubscriptionOptions options);

    static <K> KeyPublisher<K> of(List<? extends K> list) {
        return (keyConsumer, options) -> {
            if (options.snapshot()) {
                list.forEach(keyConsumer::onKey);
                keyConsumer.snapshotComplete();
            }
            return () -> {};
        };
    }
}
