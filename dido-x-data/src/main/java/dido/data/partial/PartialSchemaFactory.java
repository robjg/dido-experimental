package dido.data.partial;

import dido.data.DataSchema;
import dido.data.NoSuchFieldException;

import java.util.Collection;

public class PartialSchemaFactory<S extends PartialSchema> {

    private final Create<S> creator;

    protected PartialSchemaFactory(Create<S> creator) {
        this.creator = creator;
    }

    public interface Create<S extends PartialSchema> {

        S create(DataSchema fullSchema, int... indexes);
    }

    public S of(DataSchema schema, Collection<String> fields) {
        return of(schema, fields.toArray(new String[0]));
    }

    public S of(DataSchema schema, String... fields) {

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

    public S of(DataSchema schema, int... indexes) {

        return creator.create(schema, indexes);
    }
}
