package dido.table.internal;

import dido.data.DataSchema;
import dido.data.DidoData;
import dido.data.partial.PartialData;
import dido.flow.Receiver;
import dido.flow.util.KeyExtractor;
import dido.flow.util.KeyExtractors;
import dido.table.LiveRow;
import dido.table.LiveTable;

import java.util.*;
import java.util.function.Consumer;

public class LiveTableBasic implements LiveTable {

    private final DataSchema schema;

    private final KeyExtractor keyExtractor;

    private final Map<Comparable<?>, ArrayRowImpl> rows = new TreeMap<>();

    private final List<Consumer<LiveRow>> operations = new ArrayList<>();
    private final List<Receiver> receivers = new ArrayList<>();

    private LiveTableBasic(DataSchema schema,
                           KeyExtractor keyExtractor) {
        this.schema = schema;
        this.keyExtractor = keyExtractor;
    }

    public static class Settings {

        private DataSchema schema;

        private KeyExtractor keyExtractor;

        public Settings keyExtractor(KeyExtractor keyExtractor) {
            this.keyExtractor = keyExtractor;
            return this;
        }

        public Settings schema(DataSchema schema) {
            this.schema = schema;
            return this;
        }

        LiveTable create() {
            return new LiveTableBasic(schema, keyExtractor == null ?
                    KeyExtractors.fromFirstField().keyExtractorFor(schema) : keyExtractor);
        }
    }

    public static Settings with() {
        return new Settings();
    }

    class InternalReceiver implements Receiver {

        @Override
        public void onData(DidoData data) {
            receivers.forEach(r -> r.onData(data));
        }

        @Override
        public void onPartial(PartialData partial) {
            receivers.forEach(r -> r.onPartial(partial));
        }

        @Override
        public void onDelete(DidoData keyData) {
            receivers.forEach(r -> r.onDelete(keyData));
        }
    }

    @Override
    public void onData(DidoData data) {

        ArrayRowImpl arrayRow = rows.computeIfAbsent(keyExtractor.keyOf(data),
                key -> new ArrayRowImpl(schema, new InternalReceiver()));
        arrayRow.onData(data, operations);
    }

    @Override
    public void onPartial(PartialData partial) {

        ArrayRowImpl arrayRow = Objects.requireNonNull(
                rows.get(keyExtractor.keyOf(partial)), "Failed to find row for " + partial);

        arrayRow.onPartial(partial, operations);
    }

    @Override
    public void onDelete(DidoData keyData) {

    }

    @Override
    public LiveRow getRow(DidoData data) {
        return rows.get(keyExtractor.keyOf(data));
    }

    @Override
    public void close() {

    }
}
