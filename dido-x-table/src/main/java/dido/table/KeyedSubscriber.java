package dido.table;

import dido.data.DidoData;
import dido.data.partial.PartialUpdate;

public interface KeyedSubscriber<K extends Comparable<K>> {

    void onData(K key, DidoData data);

    void onPartial(K key, PartialUpdate data);

    void onDelete(K key, DidoData data);
}
