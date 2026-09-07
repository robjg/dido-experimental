package dido.table.internal;


import dido.data.DataSchema;
import dido.data.DidoData;
import dido.data.mutable.MutableArrayData;
import dido.data.mutable.MutableData;
import dido.data.partial.PartialData;
import dido.flow.DidoSubscription;
import dido.flow.KeyedDidoSubscriber;
import dido.flow.util.KeyedDidoDataSubscribers;
import dido.table.DataTable;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class DataTableBasic<K extends Comparable<K>> implements DataTable<K>, KeyedDidoSubscriber<K> {

    private final DataSchema schema;

    private final Map<K, MutableData> rows = new TreeMap<>();

    private final KeyedDidoDataSubscribers<K> subscribers;

    DataTableBasic(DataSchema schema) {
        this.schema = schema;
        this.subscribers = new KeyedDidoDataSubscribers<>(schema);
    }

    public static <K extends Comparable<K>> DataTableBasic<K> forSchema(DataSchema schema) {
        return new DataTableBasic<>(schema);
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
    public DidoSubscription subscribe(KeyedDidoSubscriber<K> listener) {
        return subscribers.addSubscriber(listener);
    }

    @Override
    public void onData(K key, DidoData data) {

        MutableData row = rows.computeIfAbsent(key,
                k -> MutableArrayData.copy(data));

        subscribers.onData(key, row);
    }

    @Override
    public void onPartial(K key, PartialData partial) {

        DidoData data = partial.getData();

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

        subscribers.onPartial(key, PartialData.of(row, partial.getIndices()));
    }

    @Override
    public void onDelete(K key) {

        MutableData row = rows.remove(key);
        if (row == null) {
            throw new IllegalArgumentException("No row for key " + key);
        }

        subscribers.onDelete(key);
    }
}
