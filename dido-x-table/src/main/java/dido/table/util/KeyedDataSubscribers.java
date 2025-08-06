package dido.table.util;

import dido.data.DidoData;
import dido.data.partial.PartialData;
import dido.flow.QuietlyCloseable;
import dido.table.KeyedDataSubscriber;

import java.util.ArrayList;
import java.util.List;

public class KeyedDataSubscribers<K extends Comparable<K>> implements KeyedDataSubscriber<K> {

    private KeyedDataSubscriber<? super K> existing;

    @SuppressWarnings({"rawtypes", "unchecked"})
    public QuietlyCloseable addSubscriber(KeyedDataSubscriber<? super K> additional) {

        if (existing == null) {
            existing = additional;
        }
        else if (existing instanceof SubscriberList keyedDataSubscribers) {
            keyedDataSubscribers.consumers.add(additional);
        }
        else {
            SubscriberList<K> subscriberList = new SubscriberList<>();
            subscriberList.consumers.add(existing);
            subscriberList.consumers.add(additional);
            existing = subscriberList;
        }

        return () -> remove(additional);
    }

    @Override
    public void onData(K key, DidoData data) {
        if (existing != null) {
            existing.onData(key, data);
        }
    }

    @Override
    public void onPartial(K key, PartialData data) {
        if (existing != null) {
            existing.onPartial(key, data);
        }
    }

    @Override
    public void onDelete(K key) {
        if (existing != null) {
            existing.onDelete(key);
        }
    }

    void remove(KeyedDataSubscriber<? super K> subscriber) {
        if (existing == subscriber) {
            existing = null;
        } else if (existing instanceof SubscriberList<? super K> list) {
            list.consumers.remove(subscriber);
            if (list.consumers.size() == 1) {
                existing = list.consumers.getFirst();
            }
        }
    }

    static class SubscriberList<K extends Comparable<K>> implements KeyedDataSubscriber<K> {

        private final List<KeyedDataSubscriber<? super K>> consumers = new ArrayList<>();

        @Override
        public void onData(K key, DidoData data) {
            consumers.forEach(c -> c.onData(key, data));
        }

        @Override
        public void onPartial(K key, PartialData data) {
            consumers.forEach(c -> c.onPartial(key, data));
        }

        @Override
        public void onDelete(K key) {
            consumers.forEach(c -> c.onDelete(key));
        }
    }
}
