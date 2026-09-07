package dido.table;

import dido.flow.KeyedDidoSubscriber;
import dido.flow.QuietlyCloseable;

public interface LiveTable<K extends Comparable<K>>
        extends KeyedDidoSubscriber<K>, DataTable<K>, QuietlyCloseable {

    LiveRow getRow(K key);

}
