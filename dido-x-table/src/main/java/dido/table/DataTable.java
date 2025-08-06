package dido.table;

import dido.data.DataSchema;
import dido.flow.Receiver;

public interface DataTable<K extends Comparable<K>> extends Receiver, KeyedData<K> {

    DataSchema getSchema();


}
