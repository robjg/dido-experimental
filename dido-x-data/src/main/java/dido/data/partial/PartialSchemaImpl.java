package dido.data.partial;

import dido.data.*;
import dido.data.NoSuchFieldException;
import dido.data.useful.AbstractDataSchema;

import java.util.Arrays;
import java.util.Set;

public class PartialSchemaImpl extends AbstractDataSchema implements PartialSchema {

    private final ReadSchema fullSchema;

    private final int firstIndex;

    private final int lastIndex;

    private final int[] next;

    protected PartialSchemaImpl(DataSchema fullSchema,
                                int firstIndex,
                                int lastIndex,
                                int[] next) {
        this.fullSchema = ReadSchema.from(fullSchema);
        this.firstIndex = firstIndex;
        this.lastIndex = lastIndex;
        this.next = next;
    }

    public static PartialSchema of(DataSchema schema, Set<String> fields) {
        return of(schema, fields.toArray(new String[0]));
    }

    public static PartialSchema of(DataSchema schema, String... fields) {

        int[] indexes = new int[fields.length];
        for (int i = 0; i < indexes.length; i++) {
            String fieldName = fields[i];
            int index = schema.getIndexNamed(fieldName);
            if (index == 0) {
                throw new NoSuchFieldException(fieldName, schema);
            }
            indexes[i] = index;
        }
        return of(schema, indexes);
    }

    public static PartialSchema of(DataSchema schema, int... indexes) {

        int firstIndex;
        int lastIndex;
        int[] next;

        if (indexes.length == 0) {
            firstIndex = 0;
            lastIndex = 0;
            next = new int[0];
        }
        else {
            Arrays.sort(indexes);

            firstIndex = indexes[0];
            lastIndex = indexes[indexes.length - 1];
            next = new int[lastIndex];
            int last = firstIndex;
            for (int i = 1; i < indexes.length; i++) {
                int current = indexes[i];
                next[last - 1] = current;
                last = current;
            }
            next[last - 1] = 0;
        }
        return new PartialSchemaImpl(schema, firstIndex, lastIndex, next);
    }

    @Override
    public FieldGetter getFieldGetterAt(int index) {
        return fullSchema.getFieldGetterAt(index);
    }

    @Override
    public FieldGetter getFieldGetterNamed(String name) {
        return fullSchema.getFieldGetterNamed(name);
    }

    @Override
    public int firstIndex() {
        return firstIndex;
    }

    @Override
    public int nextIndex(int index) {
        return next[index - 1];
    }

    @Override
    public int lastIndex() {
        return lastIndex;
    }

    @Override
    public SchemaField getSchemaFieldAt(int index) {
        return fullSchema.getSchemaFieldAt(index);
    }

    @Override
    public SchemaField getSchemaFieldNamed(String name) {
        return fullSchema.getSchemaFieldNamed(name);
    }

    @Override
    public DataSchema getFullSchema() {
        return fullSchema;
    }
}
