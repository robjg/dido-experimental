package dido.table.internal;

import dido.data.DataSchema;
import dido.data.DidoData;
import dido.data.partial.IndexSequence;
import dido.data.partial.PartialData;
import dido.data.util.EmptyData;
import dido.flow.DidoSubscription;
import dido.flow.KeyedDidoSubscriber;
import dido.flow.QuietlyCloseable;
import dido.flow.util.KeyedDidoDataSubscribers;
import dido.operators.Concatenator;
import dido.table.CloseableTable;
import dido.table.DataTable;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DataJoin<K extends Comparable<K>>
        implements DataTable<K>, QuietlyCloseable {

    private static class InnerJoinToken {
    }

    private static class LeftJoinToken {
    }

    private final KeyedDidoDataSubscribers<K> subscribers;

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
        this.subscribers = new KeyedDidoDataSubscribers<>(concatenator.getSchema());
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
        this.subscribers = new KeyedDidoDataSubscribers<>(concatenator.getSchema());
    }

    public static class From<K extends Comparable<K>> {

        private final DataTable<K> left;

        From(DataTable<K> left) {
            this.left = left;
        }

        public PrimaryKeys<K> primaryKeys() {

            return new PrimaryKeys<>(left);
        }

        public <K2 extends Comparable<K2>> ForeignKey<K, K2>
        foreignKey(Function<? super DidoData, ? extends K2> foreignKeyFunc) {

            return new ForeignKey<>(left, foreignKeyFunc);
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

        private final Function<? super DidoData, ? extends K2> keyExtractor;

        public ForeignKey(DataTable<K1> left,
                          Function<? super DidoData, ? extends K2> keyExtractor) {
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
    public int size() {
        return join.size();
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
    public DidoSubscription subscribe(KeyedDidoSubscriber<K> listener) {
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

        int size();

        boolean containsKey(K key);

        DidoData get(K key);

        Set<K> keySet();

        Set<Map.Entry<K, DidoData>> entrySet();
    }


    class InnerJoin implements View<K> {

        private final QuietlyCloseable leftClose;

        private final QuietlyCloseable rightClose;

        InnerJoin() {

            leftClose = left.subscribe(new KeyedDidoSubscriber<K>() {
                @Override
                public void onData(K key, DidoData data) {
                    DidoData combined = get(key);
                    if (combined != null) {
                        subscribers.onData(key, combined);
                    }
                }

                @Override
                public void onPartial(K key, PartialData partial) {
                    if (containsKey(key)) {
                        subscribers.onPartial(key, partial);
                    }
                }

                @Override
                public void onDelete(K key) {
                    DidoData rightData = right.get(key);
                    if (rightData != null) {
                        subscribers.onDelete(key);
                    }
                }
            });
            rightClose = right.subscribe(new KeyedDidoSubscriber<>() {
                @Override
                public void onData(K key, DidoData data) {
                    DidoData combined = get(key);
                    if (combined != null) {
                        subscribers.onData(key, combined);
                    }
                }

                @Override
                public void onPartial(K key, PartialData partial) {
                    if (containsKey(key)) {
                        subscribers.onPartial(key, partial);
                    }
                }

                @Override
                public void onDelete(K key) {
                    DidoData leftData = left.get(key);
                    if (leftData != null) {
                        subscribers.onDelete(key);
                    }
                }
            });
        }

        @Override
        public int size() {
            Set<K> working = new HashSet<K>(left.keySet());
            working.retainAll(right.keySet());
            return working.size();
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

            leftClose = left.subscribe(new KeyedDidoSubscriber<K>() {
                @Override
                public void onData(K key, DidoData data) {
                    subscribers.onData(key, get(key));
                }

                @Override
                public void onPartial(K key, PartialData partial) {
                    subscribers.onPartial(key, PartialData
                            .from(concatenator.concat(partial.getData(), null))
                            .withIndices(partial.getIndices()));
                }

                @Override
                public void onDelete(K key) {
                    DidoData rightData = right.get(key);
                    if (rightData == null) {
                        subscribers.onDelete(key);
                    }
                    else {
                        subscribers.onDelete(key);
                    }
                }
            });
            rightClose = right.subscribe(new KeyedDidoSubscriber<K>() {
                @Override
                public void onData(K key, DidoData data) {
                    DidoData combined = get(key);
                    if (combined != null) {
                        subscribers.onData(key, combined);
                    }
                }

                @Override
                public void onPartial(K key, PartialData partial) {
                    if (left.containsKey(key)) {
                        subscribers.onPartial(key,
                                PartialData.from(concatenator.concat(null, partial.getData()))
                                    .withIndices(partial.transpose(
                                            left.getSchema().lastIndex()).getIndices()));
                    }
                }

                @Override
                public void onDelete(K key) {
                    DidoData leftData = left.get(key);
                    if (leftData != null) {
                        subscribers.onPartial(key,
                                PartialData.from(concatenator.concat(leftData, null))
                                        .withIndices(IndexSequence.fromSchema(right.getSchema())
                                                .transpose(left.getSchema().lastIndex()).getIndices()));
                    }
                }
            });
        }

        @Override
        public int size() {
            return left.size();
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
