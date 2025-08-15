package dido.data.partial;

import dido.data.DataFactory;
import dido.data.WritableData;

public class PartialDataDelegateFactory implements DataFactory {

    private final DataFactory delegate;

    private final PartialSchema schema;

    protected PartialDataDelegateFactory(DataFactory dataFactory,
                                         PartialSchema schema) {
        this.schema = schema;
        this.delegate = dataFactory;
    }

    public static PartialDataDelegateFactory of(DataFactory dataFactory,
                                                String... fields) {
        return of(dataFactory,
                PartialSchema.of(dataFactory.getSchema(), fields));
    }

    public static PartialDataDelegateFactory of(DataFactory dataFactory,
                                                int... indexes) {
        return of(dataFactory,
                PartialSchema.of(dataFactory.getSchema(), indexes));
    }

    public static PartialDataDelegateFactory of(DataFactory dataFactory,
                                                PartialSchema schema) {
        return new PartialDataDelegateFactory(dataFactory, schema);
    }

    @Override
    public PartialSchema getSchema() {
        return schema;
    }

    @Override
    public WritableData getWritableData() {
        return delegate.getWritableData();
    }

    @Override
    public PartialData toData() {

        return new PartialDataDelegate(schema, delegate.toData());
    }

}
