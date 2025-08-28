package dido.data.partial;

import dido.data.*;
import dido.data.NoSuchFieldException;
import dido.data.useful.AbstractData;
import dido.data.useful.AbstractFieldGetter;
import dido.data.useful.AbstractFieldSetter;
import dido.data.useful.AbstractWritableData;
import dido.data.util.FieldSelectionFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * {@link PartialData} stored in an Array.
 */
public final class ArrayPartialData extends AbstractData implements PartialData {

    private final ArrayPartialSchema schema;

    private final Object[] data;

    private ArrayPartialData(ArrayPartialSchema schema, Object[] data) {
        this.schema = schema;
        this.data = data;
    }

    public static ArrayPartialSchema asArrayPartialSchema(PartialSchema schema) {

        if (schema instanceof ArrayPartialSchema) {
            return (ArrayPartialSchema) schema;

        } else {
            return new ArrayPartialSchema(schema.getFullSchema(), schema.getIndices());
        }
    }

    public static FieldSelectionFactory<ArrayPartialSchema> schemaFrom(DataSchema schema) {

        return new FieldSelectionFactory<>(schema,
                indexes -> new ArrayPartialSchema(schema, indexes));
    }

    public static PartialDataFactory factoryForSchema(PartialSchema schema) {
        return new ArrayDataFactory(asArrayPartialSchema(schema));
    }

    public static FieldSelectionFactory<PartialValuesFrom> fromSchema(DataSchema schema) {

        return new FieldSelectionFactory<>(schema,
                indexes -> new PartialValuesFrom(
                        new ArrayDataFactory(
                                new ArrayPartialSchema(schema, indexes))));
    }

    public static PartialValuesFrom withSchema(PartialSchema partialSchema) {
        return new PartialValuesFrom(factoryForSchema(partialSchema));
    }

    @Override
    public ArrayPartialSchema getSchema() {
        return schema;
    }

    @Override
    public Object getAt(int index) {
        return schema.getFieldGetterAt(index).get(this);
    }

    @Override
    public boolean hasAt(int index) {
        return getAt(index) != null;
    }

    static class ArrayDataFactory extends AbstractWritableData implements PartialDataFactory {

        private final ArrayPartialSchema schema;

        private Object[] values;

        ArrayDataFactory(ArrayPartialSchema schema) {
            this.schema = schema;
            values = new Object[schema.reIndex.size()];
        }

        @Override
        public ArrayPartialSchema getSchema() {
            return schema;
        }

        @Override
        public void clearAt(int index) {
            values[index - 1] = null;
        }

        @Override
        public void setAt(int index, Object value) {
            values[index - 1] = value;
        }

        @Override
        public void setNamed(String name, Object value) {
            int index = schema.getIndexNamed(name);
            if (index == 0) {
                throw new IllegalArgumentException(
                        "No field named " + name + ", valid field names: " + schema.getFieldNames());
            }
            setAt(index, value);
        }

        @Override
        public void clearNamed(String name) {
            int index = schema.getIndexNamed(name);
            if (index == 0) {
                throw new IllegalArgumentException(
                        "No field named " + name + ", valid field names: " + schema.getFieldNames());
            }
            clearAt(index);
        }

        @Override
        public WritableData getWritableData() {
            return this;
        }

        @Override
        public ArrayPartialData toData() {
            Object[] values = this.values;
            this.values = new Object[schema.lastIndex()];
            return new ArrayPartialData(schema, values);
        }
    }

    public static class ArrayPartialSchema extends PartialSchemaImpl
            implements PartialSchema, WriteSchema {

        /** Mapping from the data index in the full schema to the array index of values */
        final Map<Integer, Integer> reIndex = new HashMap<>();

        ArrayPartialSchema(DataSchema full, int[] indexes) {
            super(full, indexes);
            for (int i = 0; i < indexes.length; i++) {
                reIndex.put(indexes[i], i);
            }
        }

        @Override
        public FieldGetter getFieldGetterAt(int index) {
            Integer now = reIndex.get(index);
            if (now == null) {
                throw new NoSuchFieldException(index, this);
            }

            String toString = "ArrayDataGetter for [" + index + ":" + getFieldNameAt(index) + "]";

            return new AbstractFieldGetter() {
                @Override
                public Object get(DidoData data) {
                    return ((ArrayPartialData) data).data[now];
                }

                @Override
                public String toString() {
                    return toString;
                }
            };
        }

        @Override
        public int getSize() {
            return reIndex.size();
        }

        @Override
        public FieldGetter getFieldGetterNamed(String name) {
            int index = getIndexNamed(name);
            if (index == 0) {
                throw new NoSuchFieldException(name, this);
            }

            return getFieldGetterAt(index);
        }

        @Override
        public FieldSetter getFieldSetterAt(int index) {
            Integer now = reIndex.get(index);
            if (now == null) {
                throw new NoSuchFieldException(index, this);
            }

            return new AbstractFieldSetter() {
                @Override
                public void clear(WritableData writable) {
                    ((ArrayDataFactory) writable).values[now] = null;
                }

                @Override
                public void set(WritableData writable, Object value) {
                    ((ArrayDataFactory) writable).values[now] = value;
                }
            };
        }

        @Override
        public FieldSetter getFieldSetterNamed(String name) {
            int index = getIndexNamed(name);
            if (index == 0) {
                throw new NoSuchFieldException(name, this);
            }
            return getFieldSetterAt(index);
        }

    }

}
