package dido.table;

import dido.data.DidoData;
import dido.data.partial.PartialData;

public interface KeyedDataSubscriber<K extends Comparable<K>> {

    void onData(K key, DidoData data);

    void onPartial(K key, PartialData data);

    void onDelete(K key);
}
