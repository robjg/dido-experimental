package dido.table.internal;

import dido.data.DataSchema;
import dido.data.DidoData;
import dido.data.partial.PartialData;
import dido.flow.QuietlyCloseable;
import dido.flow.util.KeyExtractor;
import dido.table.DataTable;
import dido.table.KeyedDataSubscriber;

public class DataJoin<K1 extends Comparable<K1>, K2 extends Comparable<K2>> implements DataTable<K1> {

    private final DataTable<K1> left;

    private final DataTable<K2> right;

    public DataJoin(DataTable<K1> left, DataTable<K2> right) {
        this.left = left;
        this.right = right;
    }

    public static class From<K extends Comparable<K>> {

        private final DataTable<K> left;

        From(DataTable<K> left) {
            this.left = left;
        }

        public JoinOperator<K, K> primaryKeys() {

            return new PrimaryKeys<>();
        }

        public <K2 extends Comparable<K2>> JoinOperator<K, K2> foreignKey(KeyExtractor<K2> foreignKey) {

            return new ForeignKey<>();
        }

    }

    public static <K extends Comparable<K>> From<K> from(DataTable<K> from) {
        return new From<>(from);
    }

    public interface JoinOperator<K1 extends Comparable<K1>, K2 extends Comparable<K2>> {

        DataJoin<K1, K2> innerJoin(DataTable<K2> right);

        DataJoin<K1, K2> leftJoin(DataTable<K2> right);

        DataJoin<K1, K2> outerJoin(DataTable<K2> right);
    }

    static class PrimaryKeys<K extends Comparable<K>> implements JoinOperator<K, K> {

        @Override
        public DataJoin<K, K> innerJoin(DataTable<K> right) {
            return null;
        }

        @Override
        public DataJoin<K, K> leftJoin(DataTable<K> right) {
            return null;
        }

        @Override
        public DataJoin<K, K> outerJoin(DataTable<K> right) {
            return null;
        }
    }

    static class ForeignKey<K1 extends Comparable<K1>, K2 extends Comparable<K2>> implements JoinOperator<K1, K2> {

        @Override
        public DataJoin<K1, K2> innerJoin(DataTable<K2> right) {
            return null;
        }

        @Override
        public DataJoin<K1, K2> leftJoin(DataTable<K2> right) {
            return null;
        }

        @Override
        public DataJoin<K1, K2> outerJoin(DataTable<K2> right) {
            return null;
        }
    }

    @Override
    public DataSchema getSchema() {
        return null;
    }

    @Override
    public void onData(DidoData data) {

    }

    @Override
    public void onPartial(PartialData partial) {

    }

    @Override
    public void onDelete(PartialData partial) {

    }

    @Override
    public DidoData get(K1 key) {
        return null;
    }

    @Override
    public QuietlyCloseable subscribe(KeyedDataSubscriber<K1> listener) {
        return null;
    }
}
