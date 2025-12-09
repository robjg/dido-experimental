package dido.table.util;

import dido.data.DataSchema;
import dido.data.DidoData;
import dido.data.partial.PartialUpdate;
import dido.table.KeyedSubscriber;
import dido.table.KeyedSubscription;

import java.util.ArrayList;
import java.util.List;

public class KeyedDataSubscribers<K extends Comparable<K>> implements KeyedSubscriber<K> {

    private final DataSchema schema;

    private KeyedSubscriber<? super K> existing;

    public KeyedDataSubscribers(DataSchema schema) {
        this.schema = schema;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public KeyedSubscription addSubscriber(KeyedSubscriber<? super K> additional) {

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

        return new KeyedSubscription() {

            @Override
            public DataSchema getSchema() {
                return schema;
            }

            @Override
            public void close() {
                remove(additional);
            }
        };
    }

    @Override
    public void onData(K key, DidoData data) {
        if (existing != null) {
            existing.onData(key, data);
        }
    }

    @Override
    public void onPartial(K key, PartialUpdate data) {
        if (existing != null) {
            existing.onPartial(key, data);
        }
    }

    @Override
    public void onDelete(K key, DidoData data) {
        if (existing != null) {
            existing.onDelete(key, data);
        }
    }

    void remove(KeyedSubscriber<? super K> subscriber) {
        if (existing == subscriber) {
            existing = null;
        } else if (existing instanceof SubscriberList<? super K> list) {
            list.consumers.remove(subscriber);
            if (list.consumers.size() == 1) {
                existing = list.consumers.getFirst();
            }
        }
    }

    static class SubscriberList<K extends Comparable<K>> implements KeyedSubscriber<K> {

        private final List<KeyedSubscriber<? super K>> consumers = new ArrayList<>();

        @Override
        public void onData(K key, DidoData data) {
            consumers.forEach(c -> c.onData(key, data));
        }

        @Override
        public void onPartial(K key, PartialUpdate data) {
            consumers.forEach(c -> c.onPartial(key, data));
        }

        @Override
        public void onDelete(K key, DidoData data) {
            consumers.forEach(c -> c.onDelete(key, data));
        }
    }
}
