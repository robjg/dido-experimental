package dido.data.partial;

import dido.data.DataSchema;
import dido.data.ReadSchema;

import java.util.Collection;

public interface PartialSchema extends ReadSchema {

    DataSchema getFullSchema();

    static PartialSchema of(DataSchema schema, Collection<String> fields) {

        return new PartialSchemaFactory<>(PartialSchemaImpl::new)
                .of(schema, fields);
    }

    static PartialSchema of(DataSchema schema, String... fields) {

        return new PartialSchemaFactory<>(PartialSchemaImpl::new)
                .of(schema, fields);
    }

    static PartialSchema of(DataSchema schema, int... indexes) {
        return new PartialSchemaFactory<>(PartialSchemaImpl::new)
                .of(schema, indexes);
    }
}
