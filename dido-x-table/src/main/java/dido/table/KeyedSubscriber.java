package dido.table;

import dido.data.DidoData;

public interface KeyedSubscriber<K extends Comparable<K>> {

    void onData(K key, DidoData data);

    void onPartial(K key, DidoData data);

    void onDelete(K key, DidoData data);
}
