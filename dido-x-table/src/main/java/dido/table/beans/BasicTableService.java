package dido.table.beans;

import dido.data.DataSchema;
import dido.data.DidoData;
import dido.data.partial.PartialData;
import dido.data.schema.SchemaAware;
import dido.flow.KeyedDataEvent;
import dido.flow.KeyedDidoSubscriber;
import dido.flow.util.KeyUtil;
import dido.table.internal.DataTableBasic;

import java.util.function.Consumer;
import java.util.function.Function;

public class BasicTableService<K extends Comparable<K>>
        implements KeyedDidoSubscriber<K>, Consumer<Object>, SchemaAware {

    private String name;

    private DataTableBasic<K> table;

    private Function<? super DidoData, ? extends K> keyExtractor;

    private DataSchema schema;

    @Override
    public void setSchema(DataSchema schema) {
        this.schema = schema;
    }

    public DataSchema getSchema() {
        return schema;
    }

    protected void initialise(DataSchema schema) {

        table = DataTableBasic.forSchema(schema);

        if (keyExtractor == null) {
            keyExtractor = KeyUtil.fromFirstField(schema);
        }
    }


    public void start() {

        if (schema != null) {
            initialise(schema);
        }
    }

    public void stop() {

    }

    public void reset() {

        table = null;
        schema = null;
    }

    @Override
    public void accept(Object o) {

        if (table == null) {
            throw new IllegalStateException("Service not started");
        }

        if (o instanceof DidoData data) {
            if (keyExtractor == null) {
                initialise(data.getSchema());
            }
            onData(keyExtractor.apply(data), data);
        }
        else if (o instanceof KeyedDataEvent<?>) {

            @SuppressWarnings("unchecked")
            KeyedDataEvent<K> event = (KeyedDataEvent<K>) o;
            switch  (event) {
                case KeyedDataEvent.Complete<K> complete ->
                        onData(complete.getKey(), complete.getData());
                case KeyedDataEvent.Partial<K> partial ->
                        onPartial(partial.getKey(), partial.getPartial());
                case KeyedDataEvent.Delete<K> complete ->
                        onDelete(complete.getKey());
            }
        }
        else {
            throw new IllegalArgumentException("Unsupported data type: " + o.getClass().getName());
        }
    }

    @Override
    public void onData(K key, DidoData data) {
        if (table == null) {
            initialise(data.getSchema());
        }
        table.onData(key, data);
    }

    @Override
    public void onPartial(K key, PartialData partial) {
        if (table == null) {
            throw new IllegalStateException("Table not initialized");
        }

        table.onPartial(key, partial);
    }

    @Override
    public void onDelete(K key) {
        if (table == null) {
            throw new IllegalStateException("Table not initialized");
        }

        table.onDelete(key);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public DataTableBasic<K> getTable() {
        return table;
    }


    public Function<? super DidoData, ? extends K> getKeyExtractor() {
        return keyExtractor;
    }

    public void setKeyExtractor(Function<? super DidoData, ? extends K> keyExtractor) {
        this.keyExtractor = keyExtractor;
    }

    @Override
    public String toString() {
        return name == null ? "BasicTableService" : name;
    }
}
