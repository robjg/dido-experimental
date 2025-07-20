package dido.data.partial;

import dido.data.DataSchema;
import dido.data.ReadSchema;

public interface PartialSchema extends ReadSchema {

    DataSchema getFullSchema();

    static PartialSchema of(DataSchema schema, String... fields) {
        return PartialSchemaImpl.of(schema, fields);
    }

    static PartialSchema of(DataSchema schema, int... indexes) {
        return PartialSchemaImpl.of(schema, indexes);
    }
}
