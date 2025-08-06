package dido.table;

import dido.data.DidoData;
import dido.flow.QuietlyCloseable;

public interface KeyedData<K extends Comparable<K>> {

    DidoData get(K key);

    QuietlyCloseable subscribe(KeyedDataSubscriber<K> listener);
}
