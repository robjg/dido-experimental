package dido.data.partial;

import dido.data.DataSchema;
import dido.data.FieldGetter;
import dido.data.ReadSchema;
import dido.data.SchemaField;
import dido.data.useful.AbstractDataSchema;

import java.util.Arrays;

public class PartialSchemaImpl extends AbstractDataSchema implements PartialSchema {

    private final ReadSchema fullSchema;

    private final int[] indices;

    private final int firstIndex;

    private final int lastIndex;

    private final int[] next;

    protected PartialSchemaImpl(DataSchema fullSchema,
                                int... indices) {
        this.fullSchema = ReadSchema.from(fullSchema);
        this.indices = Arrays.copyOf(indices, indices.length);

        if (indices.length == 0) {
            firstIndex = 0;
            lastIndex = 0;
            next = new int[0];
        }
        else {
            Arrays.sort(indices);

            firstIndex = indices[0];
            lastIndex = indices[indices.length - 1];
            next = new int[lastIndex];
            int last = firstIndex;
            for (int i = 1; i < indices.length; i++) {
                int current = indices[i];
                next[last - 1] = current;
                last = current;
            }
            next[last - 1] = 0;
        }
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
    public int getSize() {
        return indices.length;
    }

    @Override
    public int[] getIndices() {
        return Arrays.copyOf(indices, indices.length);
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
