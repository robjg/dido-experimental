package dido.table.internal;


import dido.data.DataSchema;
import dido.data.DidoData;
import dido.data.mutable.MutableArrayData;
import dido.data.mutable.MutableData;
import dido.data.partial.PartialUpdate;
import dido.flow.DidoSubscriber;
import dido.flow.util.KeyExtractor;
import dido.flow.util.KeyExtractors;
import dido.table.DataTable;
import dido.table.KeyedSubscriber;
import dido.table.KeyedSubscription;
import dido.table.util.KeyedDataSubscribers;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class DataTableBasic<K extends Comparable<K>> implements DataTable<K>, DidoSubscriber {

    private final DataSchema schema;

    private final KeyExtractor<K> keyExtractor;

    private final Map<K, MutableData> rows = new TreeMap<>();

    private final KeyedDataSubscribers<K> subscribers;

    DataTableBasic(DataSchema schema, KeyExtractor<K> keyExtractor) {
        this.schema = schema;
        this.keyExtractor = keyExtractor;
        this.subscribers = new KeyedDataSubscribers<>(schema);
    }

    public static class Settings<K extends Comparable<K>> {

        private final DataSchema schema;

        private KeyExtractor<K> keyExtractor;

        Settings(DataSchema schema) {
            this.schema = schema;
        }

        public Settings<K> keyExtractor(KeyExtractor<K> keyExtractor) {
            this.keyExtractor = keyExtractor;
            return this;
        }

        public DataTableBasic<K> create() {
            return new DataTableBasic<>(schema,
                    keyExtractor == null ? KeyExtractors.<K>fromFirstField().keyExtractorFor(schema) :
                            keyExtractor);
        }
    }

    public static <K extends Comparable<K>> Settings<K> withSchema(DataSchema schema) {
        return new Settings<>(schema);
    }


    @Override
    public DataSchema getSchema() {
        return schema;
    }

    @Override
    public boolean containsKey(K key) {
        return rows.containsKey(key);
    }

    @Override
    public DidoData get(K key) {
        return rows.get(key);
    }

    @Override
    public Set<K> keySet() {
        return rows.keySet();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public Set<Map.Entry<K, DidoData>> entrySet() {
        return (Set) rows.entrySet();
    }

    @Override
    public KeyedSubscription tableSubscribe(KeyedSubscriber<K> listener) {
        return subscribers.addSubscriber(listener);
    }

    @Override
    public void onData(DidoData data) {

        K key = keyExtractor.keyOf(data);

        MutableData row = rows.computeIfAbsent(key,
                k -> MutableArrayData.copy(data));

        subscribers.onData(key, data);
    }

    @Override
    public void onPartial(PartialUpdate partial) {

        DidoData data = partial.getData();

        K key = keyExtractor.keyOf(data);

        MutableData row = rows.get(key);
        if (row == null) {
            throw new IllegalArgumentException("No row for key " + key);
        }

        for (int index : partial.getIndices()) {
            if (data.hasAt(index)) {
                row.setAt(index, data.getAt(index));
            }
            else {
                row.clearAt(index);
            }
        }

        subscribers.onPartial(key, partial);
    }

    @Override
    public void onDelete(DidoData keyData) {

        K key = keyExtractor.keyOf(keyData);

        MutableData row = rows.remove(key);
        if (row == null) {
            throw new IllegalArgumentException("No row for key " + key);
        }

        subscribers.onDelete(key, keyData);
    }
}
