package dido.table;

import dido.data.DidoData;
import dido.flow.DidoSubscriber;
import dido.flow.QuietlyCloseable;

public interface LiveTable<K extends Comparable<K>>
        extends DidoSubscriber, DataTable<K>, QuietlyCloseable {

    LiveRow getRow(DidoData data);

}
