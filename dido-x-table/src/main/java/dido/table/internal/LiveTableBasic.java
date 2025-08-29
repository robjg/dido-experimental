package dido.table.internal;

import dido.data.DataSchema;
import dido.data.DidoData;
import dido.data.partial.PartialData;
import dido.flow.DidoSubscriber;
import dido.flow.QuietlyCloseable;
import dido.flow.util.KeyExtractor;
import dido.flow.util.KeyExtractorProvider;
import dido.flow.util.KeyExtractors;
import dido.operators.transform.OperationDefinition;
import dido.table.DataTableSubscriber;
import dido.table.LiveRow;
import dido.table.LiveTable;
import dido.table.util.KeyedDataSubscribers;

import java.util.*;
import java.util.stream.Collectors;

public class LiveTableBasic<K extends Comparable<K>> implements LiveTable<K> {

    private final DataSchema schema;

    private final KeyExtractor<? extends K> keyExtractor;

    private final Map<K, ArrayRowImpl> rows = new TreeMap<>();

    private final LiveOperation ops;

    private final KeyedDataSubscribers<K> subscribers = new KeyedDataSubscribers<>();

    private final List<DidoSubscriber> didoSubscribers = new ArrayList<>();

    private LiveTableBasic(Settings<K> settings) {
        this.ops = settings.operationBuilder.build();
        this.schema = ops.getOutSchema();
        this.keyExtractor = settings.keyExtractor == null ?
                ((KeyExtractorProvider<K>)KeyExtractors.fromFirstField())
                        .keyExtractorFor(schema) : settings.keyExtractor;
    }

    public static class Settings<K extends Comparable<K>> {

        private final DataSchema schema;

        private final LiveOperationBuilder operationBuilder;

        private KeyExtractor<? extends K> keyExtractor;

        public Settings(DataSchema schema) {
            this.schema = schema;
            this.operationBuilder = LiveOperationBuilder.forSchema(schema);
        }

        public Settings<K> keyExtractor(KeyExtractor<? extends K> keyExtractor) {
            this.keyExtractor = keyExtractor;
            return this;
        }

        public Settings<K> addOperation(OperationDefinition opDef) {
            operationBuilder.addOp(opDef);
            return this;
        }

        LiveTable<K> create() {

            return new LiveTableBasic<>(this);
        }
    }

    public static <K extends Comparable<K>> Settings<K> forSchema(DataSchema schema) {
        return new Settings<>(schema);
    }

    class InternalDidoSubscriber implements DidoSubscriber {

        @Override
        public void onData(DidoData data) {
            didoSubscribers.forEach(r -> r.onData(data));
        }

        @Override
        public void onPartial(PartialData partial) {
            didoSubscribers.forEach(r -> r.onPartial(partial));
        }

        @Override
        public void onDelete(DidoData keyData) {
            didoSubscribers.forEach(r -> r.onDelete(keyData));
        }
    }

    @Override
    public void onData(DidoData data) {

        K key = keyExtractor.keyOf(data);
        ArrayRowImpl arrayRow = rows.computeIfAbsent(key,
                k -> new ArrayRowImpl(schema, new InternalDidoSubscriber()));

        arrayRow.onData(data, ops);
        ops.accept(arrayRow);
        subscribers.onData(key, arrayRow.asData());
    }

    @Override
    public void onPartial(PartialData partial) {

        ArrayRowImpl arrayRow = Objects.requireNonNull(
                rows.get(keyExtractor.keyOf(partial)), "Failed to find row for " + partial);

        arrayRow.onPartial(partial, ops);
    }

    @Override
    public void onDelete(DidoData keyData) {

    }

    @Override
    public LiveRow getRow(DidoData data) {
        return rows.get(keyExtractor.keyOf(data));
    }

    @Override
    public DataSchema getSchema() {
        return schema;
    }

    @Override
    public Set<K> keySet() {
        return rows.keySet();
    }

    @Override
    public Set<Map.Entry<K, DidoData>> entrySet() {
        return rows.entrySet().stream()
                .map(entry ->
                        Map.entry(entry.getKey(), entry.getValue().asData()))
                .collect(Collectors.toCollection(TreeSet::new));
    }

    @Override
    public boolean containsKey(K key) {
        return rows.containsKey(key);
    }

    @Override
    public DidoData get(K key) {
        ArrayRowImpl row = rows.get(key);
        return row == null ? null : row.asData();
    }

    @Override
    public QuietlyCloseable tableSubscribe(DataTableSubscriber<K> listener) {
        return subscribers.addSubscriber(listener);
    }

    @Override
    public void close() {

    }
}
