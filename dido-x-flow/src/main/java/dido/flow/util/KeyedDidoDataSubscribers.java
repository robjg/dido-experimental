package dido.flow.util;

import dido.data.DataSchema;
import dido.data.DidoData;
import dido.data.partial.PartialData;
import dido.flow.DidoSubscription;
import dido.flow.KeyedDidoSubscriber;

import java.util.ArrayList;
import java.util.List;

public class KeyedDidoDataSubscribers<K extends Comparable<K>> implements KeyedDidoSubscriber<K> {

    private final DataSchema schema;

    private KeyedDidoSubscriber<? super K> existing;

    public KeyedDidoDataSubscribers(DataSchema schema) {
        this.schema = schema;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public DidoSubscription addSubscriber(KeyedDidoSubscriber<? super K> additional) {

        if (existing == null) {
            existing = additional;
        }
        else if (existing instanceof KeyedDidoDataSubscribers.DidoSubscriberList keyedDataSubscribers) {
            keyedDataSubscribers.consumers.add(additional);
        }
        else {
            DidoSubscriberList<K> subscriberList = new DidoSubscriberList<>();
            subscriberList.consumers.add(existing);
            subscriberList.consumers.add(additional);
            existing = subscriberList;
        }

        return new DidoSubscription() {

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
    public void onPartial(K key, PartialData partial) {
        if (existing != null) {
            existing.onPartial(key, partial);
        }
    }

    @Override
    public void onDelete(K key) {
        if (existing != null) {
            existing.onDelete(key);
        }
    }

    void remove(KeyedDidoSubscriber<? super K> subscriber) {
        if (existing == subscriber) {
            existing = null;
        } else if (existing instanceof KeyedDidoDataSubscribers.DidoSubscriberList<? super K> list) {
            list.consumers.remove(subscriber);
            if (list.consumers.size() == 1) {
                existing = list.consumers.getFirst();
            }
        }
    }

    static class DidoSubscriberList<K extends Comparable<K>> implements KeyedDidoSubscriber<K> {

        private final List<KeyedDidoSubscriber<? super K>> consumers = new ArrayList<>();

        @Override
        public void onData(K key, DidoData data) {
            consumers.forEach(c -> c.onData(key, data));
        }

        @Override
        public void onPartial(K key, PartialData partial) {
            consumers.forEach(c -> c.onPartial(key, partial));
        }

        @Override
        public void onDelete(K key) {
            consumers.forEach(c -> c.onDelete(key));
        }
    }
}
