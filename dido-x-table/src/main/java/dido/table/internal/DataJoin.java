package dido.table.internal;

import dido.data.DataSchema;
import dido.data.DidoData;
import dido.data.util.EmptyData;
import dido.flow.QuietlyCloseable;
import dido.flow.util.KeyExtractor;
import dido.flow.util.KeyExtractorProvider;
import dido.operators.Concatenator;
import dido.table.CloseableTable;
import dido.table.DataTable;
import dido.table.KeyedSubscriber;
import dido.table.KeyedSubscription;
import dido.table.util.KeyedDataSubscribers;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class DataJoin<K extends Comparable<K>>
        implements DataTable<K>, QuietlyCloseable {

    private static class InnerJoinToken {
    }

    private static class LeftJoinToken {
    }

    private final KeyedDataSubscribers<K> subscribers;

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
        this.subscribers = new KeyedDataSubscribers<>(concatenator.getSchema());
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
        this.subscribers = new KeyedDataSubscribers<>(concatenator.getSchema());
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
            CloseableTable<K1> reKeyedRight = ForeignKeyedTable.byForeignKey(
                    left, right, keyExtractor);
            return new DataJoin<>(left, reKeyedRight,
                    new InnerJoinToken(), reKeyedRight);
        }

        public DataJoin<K1> leftJoin(DataTable<K2> right) {
            CloseableTable<K1> reKeyedRight = ForeignKeyedTable.byForeignKey(left, right, keyExtractor);
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
    public KeyedSubscription tableSubscribe(KeyedSubscriber<K> listener) {
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

            leftClose = left.tableSubscribe(new KeyedSubscriber<K>() {
                @Override
                public void onData(K key, DidoData data) {
                    DidoData combined = get(key);
                    if (combined != null) {
                        subscribers.onData(key, combined);
                    }
                }

                @Override
                public void onPartial(K key, DidoData data) {
                    if (containsKey(key)) {
                        subscribers.onPartial(key, data);
                    }
                }

                @Override
                public void onDelete(K key, DidoData data) {
                    DidoData rightData = right.get(key);
                    if (rightData != null) {
                        subscribers.onDelete(key,
                                concatenator.concat(data, rightData));
                    }
                }
            });
            rightClose = right.tableSubscribe(new KeyedSubscriber<>() {
                @Override
                public void onData(K key, DidoData data) {
                    DidoData combined = get(key);
                    if (combined != null) {
                        subscribers.onData(key, combined);
                    }
                }

                @Override
                public void onPartial(K key, DidoData data) {
                    if (containsKey(key)) {
                        subscribers.onPartial(key, data);
                    }
                }

                @Override
                public void onDelete(K key, DidoData data) {
                    DidoData leftData = left.get(key);
                    if (leftData != null) {
                        subscribers.onDelete(key,
                                concatenator.concat(leftData, data));
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

            leftClose = left.tableSubscribe(new KeyedSubscriber<K>() {
                @Override
                public void onData(K key, DidoData data) {
                    subscribers.onData(key, get(key));
                }

                @Override
                public void onPartial(K key, DidoData data) {
                    subscribers.onPartial(key, concatenator.concat(data, null));
                }

                @Override
                public void onDelete(K key, DidoData data) {
                    DidoData rightData = right.get(key);
                    if (rightData == null) {
                        subscribers.onDelete(key,
                                concatenator.concat(data, null));
                    }
                    else {
                        subscribers.onDelete(key,
                                concatenator.concat(data, rightData));
                    }
                }
            });
            rightClose = right.tableSubscribe(new KeyedSubscriber<K>() {
                @Override
                public void onData(K key, DidoData data) {
                    DidoData combined = get(key);
                    if (combined != null) {
                        subscribers.onData(key, combined);
                    }
                }

                @Override
                public void onPartial(K key, DidoData data) {
                    if (left.containsKey(key)) {
                        subscribers.onPartial(key, concatenator.concat(null, data));
                    }
                }

                @Override
                public void onDelete(K key, DidoData data) {
                    DidoData leftData = left.get(key);
                    if (leftData != null) {
                        subscribers.onPartial(key,
                                concatenator.concat(leftData, null));
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

}
