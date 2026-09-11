package dido.table;

import dido.flow.KeyedDataConsumer;
import dido.flow.QuietlyCloseable;

public interface LiveTable<K extends Comparable<K>>
        extends KeyedDataConsumer<K>, DataTable<K>, QuietlyCloseable {

    LiveRow getRow(K key);

}
