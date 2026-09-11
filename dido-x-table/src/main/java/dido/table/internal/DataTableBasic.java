package dido.table.internal;


import dido.data.DataSchema;
import dido.data.DidoData;
import dido.data.mutable.MutableArrayData;
import dido.data.mutable.MutableData;
import dido.data.partial.PartialData;
import dido.flow.*;
import dido.flow.util.KeySubscribers;
import dido.flow.util.KeyedDataSubscribers;
import dido.table.DataTable;
import dido.table.util.MutableDataHelper;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class DataTableBasic<K extends Comparable<K>>
        implements DataTable<K>, Keyed<K>, KeyedDataConsumer<K> {

    private final MutableDataHelper dataHelper;

    private final Map<K, MutableArrayData> rows = new TreeMap<>();

    private final KeyedDataSubscribers<K> dataSubscribers;

    private final KeySubscribers<K> keySubscribers;

    DataTableBasic(DataSchema fromSchema) {
        this.dataHelper = MutableDataHelper.forSchema(fromSchema);
        this.dataSubscribers = new KeyedDataSubscribers<>(fromSchema);
        this.keySubscribers = new KeySubscribers<>();
    }

    public static <K extends Comparable<K>> DataTableBasic<K> forSchema(DataSchema schema) {
        return new DataTableBasic<>(schema);
    }

    @Override
    public DataSchema getSchema() {
        return dataHelper.getSchema();
    }

    @Override
    public int size() {
        return rows.size();
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
    public QuietlyCloseable keySubscribe(KeyConsumer<? super K> keyConsumer) {
        return keySubscribers.addKeySubscriber(keyConsumer);
    }

    @Override
    public DidoSubscription subscribe(KeyedDataConsumer<? super K> consumer) {
        return dataSubscribers.addSubscriber(consumer);
    }

    @Override
    public void onData(K key, DidoData data) {

        MutableArrayData row = rows.get(key);
        if (row == null) {
            row = dataHelper.copy(data);
            rows.put(key, row);
        }
        else {
            dataHelper.update(data, row);
        }
        dataSubscribers.onData(key, row);
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

        dataSubscribers.onPartial(key, PartialData.of(row, partial.getIndices()));
    }

    @Override
    public void onDelete(K key) {

        MutableData row = rows.remove(key);
        if (row == null) {
            throw new IllegalArgumentException("No row for key " + key);
        }

        dataSubscribers.onDelete(key);
    }
}
