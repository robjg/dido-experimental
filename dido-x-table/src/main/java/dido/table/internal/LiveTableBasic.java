package dido.table.internal;

import dido.data.DataSchema;
import dido.data.DidoData;
import dido.data.partial.PartialData;
import dido.flow.DidoSubscriber;
import dido.flow.DidoSubscription;
import dido.flow.KeyedDidoSubscriber;
import dido.flow.util.KeyedDidoDataSubscribers;
import dido.operators.transform.OperationDefinition;
import dido.table.LiveRow;
import dido.table.LiveTable;

import java.util.*;
import java.util.stream.Collectors;

public class LiveTableBasic<K extends Comparable<K>> implements LiveTable<K> {

    private final DataSchema schema;

    private final Map<K, ArrayRowImpl> rows = new TreeMap<>();

    private final LiveOperation ops;

    private final KeyedDidoDataSubscribers<K> subscribers;

    private final List<DidoSubscriber> didoSubscribers = new ArrayList<>();

    private LiveTableBasic(Settings<K> settings) {
        this.ops = settings.operationBuilder.build();
        this.schema = ops.getOutSchema();
        this.subscribers = new KeyedDidoDataSubscribers<>(schema);
    }

    public static class Settings<K extends Comparable<K>> {

        private final LiveOperationBuilder operationBuilder;

        public Settings(DataSchema schema) {
            this.operationBuilder = LiveOperationBuilder.forSchema(schema);
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
    public void onData(K key, DidoData data) {

        ArrayRowImpl arrayRow = rows.computeIfAbsent(key,
                k -> new ArrayRowImpl(schema, new InternalDidoSubscriber()));

        arrayRow.onData(data, ops);
        ops.accept(arrayRow);
        subscribers.onData(key, arrayRow.asData());
    }

    @Override
    public void onPartial(K key, PartialData partial) {

        DidoData data = partial.getData();

        ArrayRowImpl arrayRow = Objects.requireNonNull(
                rows.get(key), "Failed to find row for " + partial);

        arrayRow.onPartial(partial, ops);
    }

    @Override
    public void onDelete(K key) {
        rows.remove(key);
    }

    @Override
    public LiveRow getRow(K key) {
        return rows.get(key);
    }

    @Override
    public DataSchema getSchema() {
        return schema;
    }

    @Override
    public int size() {
        return rows.size();
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
    public DidoSubscription subscribe(KeyedDidoSubscriber<K> listener) {
        return subscribers.addSubscriber(listener);
    }

    @Override
    public void close() {

    }

    @Override
    public String toString() {
        return "LiveTableBasic{size=" + rows.size() + '}';
    }
}
