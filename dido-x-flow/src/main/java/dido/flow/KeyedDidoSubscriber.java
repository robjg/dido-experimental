package dido.flow;

import dido.data.DidoData;
import dido.data.partial.PartialData;

/**
 * Something that can be attached to an
 * @param <K>
 */
public interface KeyedDidoSubscriber<K> {

    void onData(K key, DidoData data);

    void onPartial(K key, PartialData partial);

    void onDelete(K key);
}
