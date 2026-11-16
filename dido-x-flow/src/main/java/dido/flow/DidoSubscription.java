package dido.flow;

import dido.data.DataSchema;

public interface DidoSubscription extends QuietlyCloseable {

    DataSchema getSchema();

    static DidoSubscription noop(DataSchema schema) {
        return new DidoSubscription() {
            @Override
            public DataSchema getSchema() {
                return schema;
            }

            @Override
            public void close() {
            }
        };
    }
}
