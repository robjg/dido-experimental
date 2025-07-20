package dido.data.partial;

import dido.data.DidoData;

public interface PartialData extends DidoData {

    @Override
    PartialSchema getSchema();

    static PartialData of(DidoData data, String... fields) {
        return new PartialDataDelegate(PartialSchemaImpl.of(data.getSchema(), fields), data);
    }

    static PartialData of(DidoData data, int... indexes) {
        return new PartialDataDelegate(PartialSchemaImpl.of(data.getSchema(), indexes), data);
    }
}
