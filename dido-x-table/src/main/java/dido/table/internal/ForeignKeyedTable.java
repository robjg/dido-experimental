package dido.table.internal;

import dido.data.DataSchema;
import dido.data.DidoData;
import dido.data.partial.PartialData;
import dido.flow.QuietlyCloseable;
import dido.flow.util.KeyExtractor;
import dido.table.CloseableTable;
import dido.table.DataTable;
import dido.table.DataTableSubscriber;
import dido.table.util.KeyedDataSubscribers;

import java.util.*;
import java.util.stream.Collectors;

class ForeignKeyedTable<K1 extends Comparable<K1>, K2 extends Comparable<K2>>
        implements CloseableTable<K1> {

    private final Map<K1, K2> mappingTo = new HashMap<>();

    private final Map<K2, Set<K1>> mappingFrom = new HashMap<>();

    private final DataTable<K2> otherTable;

    private final KeyedDataSubscribers<K1> subscribers = new KeyedDataSubscribers<>();

    private final List<QuietlyCloseable> closeables = new ArrayList<>();

    ForeignKeyedTable(DataTable<K2> otherTable) {
        this.otherTable = otherTable;
    }

    class ReferenceTableSubscriber implements DataTableSubscriber<K2> {
        @Override
        public void onData(K2 key, DidoData data) {
            Set<K1> lefts = mappingFrom.get(key);
            if (lefts != null) {
                for (K1 left : lefts) {
                    subscribers.onData(left, data);
                }
            }
        }

        @Override
        public void onPartial(K2 key, PartialData data) {
            Set<K1> lefts = mappingFrom.get(key);
            if (lefts != null) {
                for (K1 left : lefts) {
                    subscribers.onPartial(left, data);
                }
            }
        }

        @Override
        public void onDelete(K2 key) {
            Set<K1> lefts = mappingFrom.get(key);
            if (lefts != null) {
                for (K1 left : lefts) {
                    subscribers.onDelete(left);
                }
            }
        }
    }

    public static <K1 extends Comparable<K1>, K2 extends Comparable<K2>>
    CloseableTable<K1> byForeignKey(DataTable<K1> childTable, DataTable<K2> referenceTable, KeyExtractor<K2> keyExtractor) {

        ForeignKeyedTable<K1, K2> table = new ForeignKeyedTable<>(referenceTable);

        DataTableSubscriber<K1> childSubscriber = new DataTableSubscriber<K1>() {
            @Override
            public void onData(K1 key, DidoData data) {
                K2 other = keyExtractor.keyOf(data);
                table.mappingTo.put(key, other);
                table.mappingFrom.computeIfAbsent(other, k -> new TreeSet<>()).add(key);
            }

            @Override
            public void onPartial(K1 key, PartialData data) {
                // Nothing to do.
            }

            @Override
            public void onDelete(K1 key) {
                K2 other = table.mappingTo.remove(key);
                Set<K1> set = table.mappingFrom.get(other);
                set.remove(key);
                if (set.isEmpty()) {
                    table.mappingFrom.remove(other);
                }
            }
        };

        childTable.entrySet().forEach(
                e -> childSubscriber.onData(e.getKey(), e.getValue()));

        table.closeables.add(childTable.subscribe(childSubscriber));

        table.closeables.add(referenceTable.subscribe(table.new ReferenceTableSubscriber()));

        return table;
    }

    @Override
    public DataSchema getSchema() {
        return otherTable.getSchema();
    }

    @Override
    public boolean containsKey(K1 key) {
        K2 otherKey = mappingTo.get(key);
        if (otherKey == null) {
            return false;
        }
        return otherTable.containsKey(otherKey);
    }

    @Override
    public DidoData get(K1 key) {
        K2 otherKey = mappingTo.get(key);
        if (otherKey == null) {
            return null;
        }
        return otherTable.get(otherKey);
    }

    @Override
    public Set<K1> keySet() {
        return mappingTo.entrySet().stream()
                .filter(e -> otherTable.containsKey(e.getValue()))
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<Map.Entry<K1, DidoData>> entrySet() {
        return mappingTo.entrySet().stream()
                .filter(e -> otherTable.containsKey(e.getValue()))
                .map(e -> Map.entry(e.getKey(), otherTable.get(e.getValue())))
                .collect(Collectors.toSet());
    }

    @Override
    public QuietlyCloseable subscribe(DataTableSubscriber<K1> listener) {
        return subscribers.addSubscriber(listener);
    }

    @Override
    public void close() {
        closeables.forEach(QuietlyCloseable::close);
    }
}
