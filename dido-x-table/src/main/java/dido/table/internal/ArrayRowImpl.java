package dido.table.internal;

import dido.data.DataSchema;
import dido.data.DidoData;
import dido.data.NoSuchFieldException;
import dido.data.SchemaField;
import dido.table.LiveRow;
import dido.table.LiveValue;

public class ArrayRowImpl implements LiveRow {

    private final DataSchema schema;

    private final LiveValue[] liveValues;

    public ArrayRowImpl(DataSchema schema) {
        this.schema = schema;
        liveValues = new LiveValue[schema.lastIndex()];
        for (int i = schema.firstIndex(); i > 0; i = schema.nextIndex(i)) {
            liveValues[i -1] = new ObjectLiveValue();
        }
    }

    void load(DidoData data) {

        DataSchema loadSchema = data.getSchema();
        for (SchemaField field : schema.getSchemaFields()) {

            if (loadSchema.hasNamed(field.getName())) {
                liveValues[field.getIndex() - 1].set(data.getNamed(field.getName()));
            }
        }
    }

    @Override
    public DataSchema getSchema() {
        return schema;
    }

    @Override
    public LiveValue getValueAt(int index) {
        try {
            LiveValue liveValue = liveValues[index - 1];
            if (liveValue == null) {
                throw new NoSuchFieldException(index, schema);
            }
            return liveValue;
        }
        catch (IndexOutOfBoundsException e) {
            throw new NoSuchFieldException(index, schema);
        }
    }

    @Override
    public LiveValue getValueNamed(String name) {
        int index = schema.getIndexNamed(name);
        if (index == 0) {
            throw new NoSuchFieldException(name, schema);
        }
        return liveValues[index -1];
    }
}
