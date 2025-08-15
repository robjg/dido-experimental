package dido.table.internal;

import dido.data.DataSchema;
import dido.data.DidoData;
import dido.data.partial.PartialData;
import dido.data.util.EmptyData;
import dido.flow.QuietlyCloseable;
import dido.flow.util.KeyExtractor;
import dido.flow.util.KeyExtractorProvider;
import dido.operators.Concatenator;
import dido.table.DataTable;
import dido.table.DataTableSubscriber;
import dido.table.util.KeyedDataSubscribers;

import java.util.*;
import java.util.stream.Collectors;

public class DataJoin<K extends Comparable<K>>
        implements DataTable<K>, QuietlyCloseable {

    private static class InnerJoinToken {
    }

    private static class LeftJoinToken {
    }

    private final KeyedDataSubscribers<K> subscribers = new KeyedDataSubscribers<>();

    private final Concatenator concatenator;

    private final DataTable<K> left;

    private final DataTable<K> right;

    private final View<K> join;

    private final QuietlyCloseable additionalClosable;

    private DataJoin(DataTable<K> left,
                     DataTable<K> right,
                     InnerJoinToken joinToken) {
        this(left, right, joinToken, null);
    }

    private DataJoin(DataTable<K> left,
                     DataTable<K> right,
                     InnerJoinToken ignored,
                     QuietlyCloseable additionalClosable) {
        this.left = left;
        this.right = right;
        this.join = new InnerJoin();

        this.concatenator = Concatenator.fromSchemas(left.getSchema(), right.getSchema());
        this.additionalClosable = additionalClosable;
    }

    private DataJoin(DataTable<K> left,
                     DataTable<K> right,
                     LeftJoinToken joinToken) {
        this(left, right, joinToken, null);
    }

    private DataJoin(DataTable<K> left,
                     DataTable<K> right,
                     LeftJoinToken ignored,
                     QuietlyCloseable additionalClosable) {
        this.left = left;
        this.right = right;
        this.join = new LeftJoin();

        this.concatenator = Concatenator.fromSchemas(left.getSchema(), right.getSchema());
        this.additionalClosable = additionalClosable;
    }

    public static class From<K extends Comparable<K>> {

        private final DataTable<K> left;

        From(DataTable<K> left) {
            this.left = left;
        }

        public PrimaryKeys<K> primaryKeys() {

            return new PrimaryKeys<>(left);
        }

        public <K2 extends Comparable<K2>> ForeignKey<K, K2> foreignKey(KeyExtractorProvider<K2> foreignKey) {

            return new ForeignKey<>(left, foreignKey.keyExtractorFor(left.getSchema()));
        }

    }

    public static <K extends Comparable<K>> From<K> from(DataTable<K> from) {
        return new From<>(from);
    }

    public static class PrimaryKeys<K extends Comparable<K>> {

        private final DataTable<K> left;

        public PrimaryKeys(DataTable<K> left) {
            this.left = Objects.requireNonNull(left);
        }

        public DataJoin<K> innerJoin(DataTable<K> right) {
            return new DataJoin<>(left, right, new InnerJoinToken());
        }

        public DataJoin<K> leftJoin(DataTable<K> right) {
            return new DataJoin<>(left, right, new LeftJoinToken());
        }

        public DataJoin<K> outerJoin(DataTable<K> right) {
            return null;
        }
    }

    public static class ForeignKey<K1 extends Comparable<K1>, K2 extends Comparable<K2>> {

        private final DataTable<K1> left;

        private final KeyExtractor<K2> keyExtractor;

        public ForeignKey(DataTable<K1> left, KeyExtractor<K2> keyExtractor) {
            this.keyExtractor = Objects.requireNonNull(keyExtractor);
            this.left = Objects.requireNonNull(left);
        }

        public DataJoin<K1> innerJoin(DataTable<K2> right) {
            ForeignKeyedTable<K1, K2> reKeyedRight = new ForeignKeyedTable<>(left, right, keyExtractor);
            return new DataJoin<>(left, reKeyedRight,
                    new InnerJoinToken(), reKeyedRight);
        }

        public DataJoin<K1> leftJoin(DataTable<K2> right) {
            ForeignKeyedTable<K1, K2> reKeyedRight = new ForeignKeyedTable<>(left, right, keyExtractor);
            return new DataJoin<>(left, reKeyedRight,
                    new LeftJoinToken(), reKeyedRight);
        }

        public DataJoin<K1> outerJoin(DataTable<K2> right) {
            return null;
        }
    }

    @Override
    public DataSchema getSchema() {
        return concatenator.getSchema();
    }

    @Override
    public boolean containsKey(K key) {
        return join.containsKey(key);
    }

    @Override
    public DidoData get(K key) {
        return join.get(key);
    }

    @Override
    public Set<K> keySet() {
        return join.keySet();
    }

    @Override
    public Set<Map.Entry<K, DidoData>> entrySet() {
        return join.entrySet();
    }

    @Override
    public QuietlyCloseable subscribe(DataTableSubscriber<K> listener) {
        return subscribers.addSubscriber(listener);
    }

    @Override
    public void close() {
        join.close();
        if (additionalClosable != null) {
            additionalClosable.close();
        }
    }

    interface View<K> extends QuietlyCloseable {

        boolean containsKey(K key);

        DidoData get(K key);

        Set<K> keySet();

        Set<Map.Entry<K, DidoData>> entrySet();
    }


    class InnerJoin implements View<K> {

        private final QuietlyCloseable leftClose;

        private final QuietlyCloseable rightClose;

        InnerJoin() {

            leftClose = left.subscribe(new DataTableSubscriber<K>() {
                @Override
                public void onData(K key, DidoData data) {
                    DidoData combined = get(key);
                    if (combined != null) {
                        subscribers.onData(key, combined);
                    }
                }

                @Override
                public void onPartial(K key, PartialData data) {
                    if (containsKey(key)) {
                        subscribers.onPartial(key, data);
                    }
                }

                @Override
                public void onDelete(K key) {
                    if (right.containsKey(key)) {
                        subscribers.onDelete(key);
                    }
                }
            });
            rightClose = right.subscribe(new DataTableSubscriber<K>() {
                @Override
                public void onData(K key, DidoData data) {
                    DidoData combined = get(key);
                    if (combined != null) {
                        subscribers.onData(key, combined);
                    }
                }

                @Override
                public void onPartial(K key, PartialData data) {
                    if (containsKey(key)) {
                        subscribers.onPartial(key, data);
                    }
                }

                @Override
                public void onDelete(K key) {
                    if (right.containsKey(key)) {
                        subscribers.onDelete(key);
                    }
                }
            });
        }

        @Override
        public boolean containsKey(K key) {
            return left.containsKey(key) && right.containsKey(key);
        }

        @Override
        public DidoData get(K key) {
            DidoData leftData = left.get(key);
            DidoData rightData = right.get(key);
            if (leftData == null || rightData == null) {
                return null;
            } else {
                return concatenator.concat(leftData, rightData);
            }
        }

        @Override
        public Set<K> keySet() {
            Set<K> keys = new TreeSet<>(left.keySet());
            keys.retainAll(right.keySet());
            return keys;
        }

        @Override
        public Set<Map.Entry<K, DidoData>> entrySet() {

            return keySet().stream()
                    .map(k -> Map.entry(k, get(k)))
                    .collect(Collectors.toSet());
        }

        @Override
        public void close() {
            leftClose.close();
            rightClose.close();
        }
    }

    class LeftJoin implements View<K> {

        private final QuietlyCloseable leftClose;

        private final QuietlyCloseable rightClose;

        LeftJoin() {

            leftClose = left.subscribe(new DataTableSubscriber<K>() {
                @Override
                public void onData(K key, DidoData data) {
                    subscribers.onData(key, get(key));
                }

                @Override
                public void onPartial(K key, PartialData data) {
                    subscribers.onPartial(key, data);
                }

                @Override
                public void onDelete(K key) {
                    subscribers.onDelete(key);
                }
            });
            rightClose = right.subscribe(new DataTableSubscriber<K>() {
                @Override
                public void onData(K key, DidoData data) {
                    DidoData combined = get(key);
                    if (combined != null) {
                        subscribers.onData(key, combined);
                    }
                }

                @Override
                public void onPartial(K key, PartialData data) {
                    if (containsKey(key)) {
                        subscribers.onPartial(key, data);
                    }
                }

                @Override
                public void onDelete(K key) {
                    if (containsKey(key)) {
                        subscribers.onPartial(key,
                                PartialData.of(EmptyData.withSchema(getSchema()),
                                        right.getSchema().getFieldNames()));
                    }
                }
            });
        }

        @Override
        public boolean containsKey(K key) {
            return left.containsKey(key);
        }

        @Override
        public DidoData get(K key) {
            DidoData leftData = left.get(key);
            if (leftData == null) {
                return null;
            }
            DidoData rightData = right.get(key);
            return concatenator.concat(leftData,
                    Objects.requireNonNullElseGet(rightData,
                            () -> EmptyData.withSchema(right.getSchema())));
        }

        @Override
        public Set<K> keySet() {
            return left.keySet();
        }

        @Override
        public Set<Map.Entry<K, DidoData>> entrySet() {

            return keySet().stream()
                    .map(k -> Map.entry(k, get(k)))
                    .collect(Collectors.toSet());
        }

        @Override
        public void close() {
            leftClose.close();
            rightClose.close();
        }
    }

    static class ForeignKeyedTable<K1 extends Comparable<K1>, K2 extends Comparable<K2>>
            implements DataTable<K1>, DataTableSubscriber<K1>, QuietlyCloseable {

        private final Map<K1, K2> mappingTo = new HashMap<>();

        private final Map<K2, Set<K1>> mappingFrom = new HashMap<>();

        private final DataTable<K2> otherTable;

        private final KeyExtractor<K2> keyExtractor;

        private final KeyedDataSubscribers<K1> subscribers = new KeyedDataSubscribers<>();

        private final QuietlyCloseable closeables;

        ForeignKeyedTable(DataTable<K1> keyTable, DataTable<K2> otherTable, KeyExtractor<K2> keyExtractor) {
            this.otherTable = otherTable;
            this.keyExtractor = keyExtractor;

            keyTable.entrySet().forEach(
                    e -> onData(e.getKey(), e.getValue()));

            QuietlyCloseable keySubscribeClose = keyTable.subscribe(this);

            QuietlyCloseable otherSubscribeClose = otherTable.subscribe(new DataTableSubscriber<K2>() {
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
            });

            this.closeables = QuietlyCloseable.of(keySubscribeClose, otherSubscribeClose);
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
        public void onData(K1 key, DidoData data) {
            K2 other = keyExtractor.keyOf(data);
            mappingTo.put(key, other);
            mappingFrom.computeIfAbsent(other, k -> new TreeSet<>()).add(key);
        }

        @Override
        public void onPartial(K1 key, PartialData data) {
            // Nothing to do.
        }

        @Override
        public void onDelete(K1 key) {
            K2 other = mappingTo.remove(key);
            Set<K1> set = mappingFrom.get(other);
            set.remove(key);
            if (set.isEmpty()) {
                mappingFrom.remove(other);
            }
        }

        @Override
        public void close() {
            closeables.close();
        }
    }

}
