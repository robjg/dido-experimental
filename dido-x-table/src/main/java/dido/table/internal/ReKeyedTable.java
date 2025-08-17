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

/**
 *
 * @param <K1>
 * @param <K2>
 */
class ReKeyedTable<K1 extends Comparable<K1>, K2 extends Comparable<K2>>
        implements CloseableTable<K1> {

    private final Map<K1, K2> mappingTo = new TreeMap<>();

    private final Map<K2, K1> mappingFrom = new HashMap<>();

    private final Map<K1, Set<K2>> otherMappings = new HashMap<>();

    private final DataTable<K2> otherTable;

    private final KeyedDataSubscribers<K1> subscribers = new KeyedDataSubscribers<>();

    private final List<QuietlyCloseable> closeables = new ArrayList<>();

    ReKeyedTable(DataTable<K2> otherTable) {
        this.otherTable = otherTable;
    }

    public static <K1 extends Comparable<K1>, K2 extends Comparable<K2>>
    CloseableTable<K1> remapKey(DataTable<K2> existingTable, KeyExtractor<K1> keyExtractor) {

        ReKeyedTable<K1, K2> table = new ReKeyedTable<>(existingTable);

        DataTableSubscriber<K2> existingSubscriber = new DataTableSubscriber<>() {
            @Override
            public void onData(K2 other, DidoData data) {
                K1 key = keyExtractor.keyOf(data);
                K2 existing = table.mappingTo.put(key, other);
                if (existing != null && !existing.equals(other)) {
                    table.otherMappings.computeIfAbsent(key, k -> new LinkedHashSet<>()).add(existing);
                }
                table.mappingFrom.put(other, key);
                table.subscribers.onData(key, data);
            }

            @Override
            public void onPartial(K2 key, PartialData partial) {
                K1 left = table.mappingFrom.get(key);
                table.subscribers.onPartial(left, partial);
            }

            @Override
            public void onDelete(K2 other) {
                K1 left = table.mappingFrom.remove(other);
                Set<K2> others = table.otherMappings.get(left);
                if (others == null) {
                    table.mappingTo.remove(left);
                    table.subscribers.onDelete(left);
                }
                else {
                    K2 next = others.iterator().next();
                    others.remove(next);
                    if (others.isEmpty()) {
                        table.otherMappings.remove(left);
                    }
                    table.mappingTo.put(left, next);
                    table.mappingFrom.put(next, left);
                    table.subscribers.onData(left, existingTable.get(next));
                }

            }
        };

        existingTable.entrySet().forEach(
                e -> existingSubscriber.onData(e.getKey(), e.getValue()));

        table.closeables.add(existingTable.subscribe(existingSubscriber));

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
        return new TreeSet<>(mappingTo.keySet());
    }

    @Override
    public Set<Map.Entry<K1, DidoData>> entrySet() {
        Map<K1, DidoData> remap = mappingTo.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> otherTable.get(e.getValue()),
                        (l, r) -> l,
                        TreeMap::new));
        return remap.entrySet();
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
