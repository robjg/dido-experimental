package dido.data.partial;

import dido.data.DataSchema;
import dido.data.DidoData;

import java.util.Collection;

public interface PartialData extends DidoData {

    @Override
    PartialSchema getSchema();

    static PartialData of(DidoData data, Collection<String> fields) {
        return new PartialDataDelegate(PartialSchema.of(data.getSchema(), fields), data);
    }

    static PartialData of(DidoData data, String... fields) {
        return new PartialDataDelegate(PartialSchema.of(data.getSchema(), fields), data);
    }

    static PartialData of(DidoData data, int... indexes) {
        return new PartialDataDelegate(PartialSchema.of(data.getSchema(), indexes), data);
    }

    static PartialValuesFrom withSchema(PartialSchema schema) {
        return ArrayPartialData.withSchema(schema);
    }

    static FieldSelectionFactory<PartialValuesFrom> fromSchema(DataSchema schema) {
        return ArrayPartialData.fromSchema(schema);
    }
}
