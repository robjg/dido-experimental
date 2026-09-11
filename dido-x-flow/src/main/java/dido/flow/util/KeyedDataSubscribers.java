package dido.flow.util;

import dido.data.DataSchema;
import dido.data.DidoData;
import dido.data.partial.PartialData;
import dido.flow.DidoSubscription;
import dido.flow.KeyedDataConsumer;

import java.util.ArrayList;
import java.util.List;

public class KeyedDataSubscribers<K extends Comparable<K>> implements KeyedDataConsumer<K> {

    private final DataSchema schema;

    private KeyedDataConsumer<? super K> existing;

    public KeyedDataSubscribers(DataSchema schema) {
        this.schema = schema;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public DidoSubscription addSubscriber(KeyedDataConsumer<? super K> additional) {

        if (existing == null) {
            existing = additional;
        }
        else if (existing instanceof DidoSubscriberList keyedDataSubscribers) {
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

    void remove(KeyedDataConsumer<? super K> subscriber) {
        if (existing == subscriber) {
            existing = null;
        } else if (existing instanceof DidoSubscriberList<? super K> list) {
            list.consumers.remove(subscriber);
            if (list.consumers.size() == 1) {
                existing = list.consumers.getFirst();
            }
        }
    }

    static class DidoSubscriberList<K> implements KeyedDataConsumer<K> {

        private final List<KeyedDataConsumer<? super K>> consumers = new ArrayList<>();

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
