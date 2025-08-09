package dido.table.internal;

import dido.data.DataSchema;
import dido.data.DidoData;
import dido.data.partial.PartialData;
import dido.flow.QuietlyCloseable;
import dido.flow.util.KeyExtractor;
import dido.flow.util.KeyExtractorProvider;
import dido.operators.Concatenator;
import dido.table.DataTable;
import dido.table.DataTableSubscriber;
import dido.table.util.KeyedDataSubscribers;

import java.util.*;

public class DataJoin<K extends Comparable<K>>
        implements DataTable<K>, QuietlyCloseable {

    private static class InnerJoinToken {}

    private final KeyedDataSubscribers<K> subscribers = new KeyedDataSubscribers<K>();

    private final Concatenator concatenator;

    private final DataTable<K> left;

    private final DataTable<K> right;

    private final View<K> join;

    private final QuietlyCloseable additionalClosable;

    private DataJoin(DataTable<K> left,
                     DataTable<K> right,
                     InnerJoinToken ignored) {
        this.left = left;
        this.right = right;
        this.join = new InnerJoin();

        this.concatenator = Concatenator.fromSchemas(left.getSchema(), right.getSchema());
        this.additionalClosable = null;
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
            return null;
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
            ForeignKeyedTable<K1, K2> reKeyedRight = new ForeignKeyedTable<>(right, keyExtractor);
            QuietlyCloseable leftSubscribeClose = left.subscribe(reKeyedRight);
            return new DataJoin<>(left, reKeyedRight, new InnerJoinToken(),
                    QuietlyCloseable.of(leftSubscribeClose, reKeyedRight));
        }

        public DataJoin<K1> leftJoin(DataTable<K2> right) {
            return null;
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
    }


    class InnerJoin implements View<K> {

        private final QuietlyCloseable leftClose;

        private final QuietlyCloseable rightClose;

        InnerJoin() {

            leftClose = left.subscribe(new DataTableSubscriber<K>() {
                @Override
                public void onData(K key, DidoData data) {
                    DidoData combined = join.get(key);
                    if (combined != null) {
                        subscribers.onData(key, combined);
                    }
                }

                @Override
                public void onPartial(K key, PartialData data) {
                    if (join.containsKey(key)) {
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
                    DidoData combined = join.get(key);
                    if (combined != null) {
                        subscribers.onData(key, combined);
                    }
                }

                @Override
                public void onPartial(K key, PartialData data) {
                    if (join.containsKey(key)) {
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
            }
            else {
                return concatenator.concat(leftData, rightData);
            }
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

        private final QuietlyCloseable otherTableSubscriptionClose;

        ForeignKeyedTable(DataTable<K2> otherTable, KeyExtractor<K2> keyExtractor) {
            this.otherTable = otherTable;
            this.keyExtractor = keyExtractor;
            this.otherTableSubscriptionClose = otherTable.subscribe(new DataTableSubscriber<K2>() {
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
        public QuietlyCloseable subscribe(DataTableSubscriber<K1> listener) {
            return subscribers.addSubscriber(listener);
        }

        @Override
        public void onData(K1 key, DidoData data) {
            K2 other = keyExtractor.keyOf(data);
            mappingTo.put(key, other);
            mappingFrom.computeIfAbsent(other, k -> new HashSet<>()).add(key);
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
            otherTableSubscriptionClose.close();
        }
    }


}
