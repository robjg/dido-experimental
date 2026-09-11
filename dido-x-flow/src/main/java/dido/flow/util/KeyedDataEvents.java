package dido.flow.util;

import dido.data.DidoData;
import dido.data.partial.PartialData;
import dido.flow.KeyedDataEvent;

abstract public class KeyedDataEvents<K> {

    private final K key;

    protected KeyedDataEvents(K key) {
        this.key = key;
    }

    public K getKey() {
        return key;
    }

    static class Complete<K> extends KeyedDataEvents<K> implements KeyedDataEvent.Complete<K> {

        private final DidoData data;

        Complete(K key, DidoData data) {

            super(key);
            this.data = data;
        }

        @Override
        public DidoData getData() {
            return data;
        }

    }

    static class Partial<K> extends KeyedDataEvents<K> implements KeyedDataEvent.Partial<K> {

        private final PartialData partial;

        Partial(K key, PartialData partial) {

            super(key);
            this.partial = partial;
        }

        @Override
        public PartialData getPartial() {
            return partial;
        }
    }

    static class Delete<K> extends KeyedDataEvents<K> implements KeyedDataEvent.Delete<K> {

       Delete(K key) {
           super(key);
       }
    }

    public static <K> KeyedDataEvent.Complete<K> complete(K key, DidoData data) {
        return new Complete<>(key, data);
    }

    public static <K> KeyedDataEvent.Partial<K> partial(K key, PartialData partial) {
        return new Partial<>(key, partial);
    }

    public static <K>KeyedDataEvent.Delete<K> delete(K key) {
        return new Delete<>(key);
    }
}
