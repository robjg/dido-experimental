package dido.table;

import dido.data.DataSchema;
import dido.data.DidoData;
import dido.flow.QuietlyCloseable;

public interface DataTable<K extends Comparable<K>> {

    DataSchema getSchema();

    boolean containsKey(K key);

    DidoData get(K key);

    QuietlyCloseable subscribe(DataTableSubscriber<K> listener);

}
