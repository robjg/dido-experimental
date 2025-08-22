package dido.flow;

import dido.data.DataSchema;

public interface DidoSubscription extends QuietlyCloseable {

    DataSchema getSchema();
}
