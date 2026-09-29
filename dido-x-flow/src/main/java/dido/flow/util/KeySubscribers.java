package dido.flow.util;

import dido.flow.KeyConsumer;
import dido.flow.QuietlyCloseable;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for handling Subscribers of keyed data.
 * This isn't thread safe.
 *
 * @param <K> The key type.
 */
public class KeySubscribers<K> implements KeyConsumer<K> {

    private KeyConsumer<? super K> existingKey;

    @SuppressWarnings({"rawtypes", "unchecked"})
    public QuietlyCloseable addKeySubscriber(KeyConsumer<? super K> additional) {

        if (existingKey == null) {
            existingKey = additional;
        }
        else if (existingKey instanceof KeySubscribers.KeyConsumerList subscribers) {
            subscribers.consumers.add(additional);
        }
        else {
            KeyConsumerList<K> subscriberList = new KeyConsumerList<>();
            subscriberList.consumers.add(existingKey);
            subscriberList.consumers.add(additional);
            existingKey = subscriberList;
        }

        return () -> removeKeySubscriber(additional);
    }

    @Override
    public void onAvailable(K key) {
        if (existingKey != null) {
            existingKey.onAvailable(key);
        }
    }

    @Override
    public void onRemoved(K key) {
        if (existingKey != null) {
            existingKey.onRemoved(key);
        }
    }

    public boolean hasSubscribers() {
        return existingKey != null;
    }

    void removeKeySubscriber(KeyConsumer<? super K> subscriber) {
        if (existingKey == subscriber) {
            existingKey = null;
        } else if (existingKey instanceof KeySubscribers.KeyConsumerList<? super K> list) {
            list.consumers.remove(subscriber);
            if (list.consumers.size() == 1) {
                existingKey = list.consumers.getFirst();
            }
        }
    }

    static class KeyConsumerList<K> implements KeyConsumer<K> {

        private final List<KeyConsumer<? super K>> consumers = new ArrayList<>();

        @Override
        public void onAvailable(K key) {
            consumers.forEach(c -> c.onAvailable(key));
        }

        @Override
        public void onRemoved(K key) {
            consumers.forEach(c -> c.onRemoved(key));
        }
    }
}
