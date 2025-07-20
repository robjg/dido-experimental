package dido.data.partial;

import dido.data.DidoData;
import dido.data.useful.AbstractData;

public class PartialDataDelegate extends AbstractData implements PartialData {

    private final PartialSchema schema;

    private final DidoData data;

    PartialDataDelegate(PartialSchema schema,
                        DidoData data) {
        this.schema = schema;
        this.data = data;
    }

    @Override
    public PartialSchema getSchema() {
        return schema;
    }

    @Override
    public Object getAt(int index) {
        return data.getAt(index);
    }
}
