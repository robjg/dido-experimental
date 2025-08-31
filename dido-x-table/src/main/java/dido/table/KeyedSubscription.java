package dido.table;

import dido.data.DataSchema;
import dido.flow.QuietlyCloseable;

public interface KeyedSubscription extends QuietlyCloseable {

    DataSchema getSchema();

}
