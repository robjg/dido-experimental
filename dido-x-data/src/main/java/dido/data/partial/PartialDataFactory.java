package dido.data.partial;

import dido.data.DataFactory;
import dido.data.WritableData;

public class PartialDataFactory implements DataFactory {

    private final DataFactory delegate;

    private final PartialSchema schema;

    protected PartialDataFactory(DataFactory dataFactory,
                              PartialSchema schema) {
        this.schema = schema;
        this.delegate = dataFactory;
    }

    public static PartialDataFactory of(DataFactory dataFactory,
                                 String... fields) {
        return of(dataFactory,
                PartialSchemaImpl.of(dataFactory.getSchema(), fields));
    }

    public static PartialDataFactory of(DataFactory dataFactory,
                                 int... indexes) {
        return of(dataFactory,
                PartialSchemaImpl.of(dataFactory.getSchema(), indexes));
    }

    public static PartialDataFactory of(DataFactory dataFactory,
                                 PartialSchema schema) {
        return new PartialDataFactory(dataFactory, schema);
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
