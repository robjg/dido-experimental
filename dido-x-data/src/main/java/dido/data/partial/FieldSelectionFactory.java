package dido.data.partial;

import dido.data.DataSchema;
import dido.data.NoSuchFieldException;

import java.util.Collection;
import java.util.function.Function;

public class FieldSelectionFactory<C> {

    private final DataSchema schema;

    private final Function<int[], C> creator;

    public FieldSelectionFactory(DataSchema schema, Function<int[], C> creator) {
        this.schema = schema;
        this.creator = creator;
    }

    public C of(Collection<String> fields) {
        return of(fields.toArray(new String[0]));
    }

    public C of(String... fields) {

        int[] indexes = new int[fields.length];
        for (int i = 0; i < indexes.length; i++) {
            String fieldName = fields[i];
            int index = schema.getIndexNamed(fieldName);
            if (index == 0) {
                throw new NoSuchFieldException(fieldName, schema);
            }
            indexes[i] = index;
        }
        return of(indexes);
    }

    public C of(int... indexes) {

        return creator.apply(indexes);
    }

}
