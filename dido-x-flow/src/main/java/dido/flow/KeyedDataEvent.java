package dido.flow;

import dido.data.DidoData;
import dido.data.partial.PartialData;
import dido.flow.util.KeyedDataEvents;

public sealed interface KeyedDataEvent<K>
        permits KeyedDataEvent.Complete, KeyedDataEvent.Partial, KeyedDataEvent.Delete {

    enum Type {
        COMPLETE,
        PARTIAL,
        DELETE
    }

    Type getType();

    K getKey();

    non-sealed interface Complete<K> extends KeyedDataEvent<K> {

        default Type getType() {
            return Type.COMPLETE;
        }

        DidoData getData();
    }

    non-sealed interface Partial<K> extends KeyedDataEvent<K> {

        default Type getType() {
            return Type.PARTIAL;
        }

        PartialData getPartial();
    }

    non-sealed interface Delete<K> extends KeyedDataEvent<K> {

        default Type getType() {
            return Type.DELETE;
        }
    }

    static <K> Complete<K> complete(K key, DidoData data) {
        return KeyedDataEvents.complete(key, data);
    }

    static <K> Partial<K> partial(K key, PartialData partial) {
        return KeyedDataEvents.partial(key, partial);
    }

    static <K> Delete<K> delete(K key) {
        return KeyedDataEvents.delete(key);
    }

}
