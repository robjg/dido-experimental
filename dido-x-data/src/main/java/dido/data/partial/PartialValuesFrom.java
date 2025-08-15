package dido.data.partial;

import dido.data.*;

public class PartialValuesFrom {

    private final PartialDataFactory dataFactory;

    private final FieldSetter[] setters;

    public PartialValuesFrom(PartialDataFactory dataFactory) {
        this.dataFactory = dataFactory;
        DataSchema schema = dataFactory.getSchema();
        WriteStrategy writeStrategy = WriteStrategy.fromSchema(schema);
        setters = new FieldSetter[schema.lastIndex()];
        int i = 0;
        for (int index = schema.firstIndex(); index > 0; index = schema.nextIndex(index)) {
            setters[i++] = writeStrategy.getFieldSetterAt(index);
        }
    }

    public PartialData of(Object... values) {
        WritableData writableData = dataFactory.getWritableData();
        for (int i = 0; i < values.length; ++i) {
            Object value = values[i];
            if (value != null) {
                setters[i].set(writableData, value);
            }
        }
        return dataFactory.toData();
    }

}
