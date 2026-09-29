package dido.flow.util;

import dido.data.DidoData;
import dido.data.partial.PartialData;
import dido.flow.KeyedDataConsumer;
import dido.flow.KeyedDataEvent;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * Helper methods for {@link KeyedDataEvent}s.
 *
 * @param <K>
 */
public abstract class KeyedDataEvents<K> {

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

        @Override
        public boolean equals(Object o) {
            if (o instanceof KeyedDataEvents.Complete <?> event) {
                return Objects.equals(getKey(), event.getKey()) &&
                        Objects.equals(data, event.data);
            }
            else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return Objects.hash(getType(), getKey(), data);
        }

        @Override
        public String toString() {
            return getType() + " [" + getKey() + "]:" + data;
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

        @Override
        public boolean equals(Object o) {
            if (o instanceof KeyedDataEvents.Partial <?> event) {
                return Objects.equals(getKey(), event.getKey()) &&
                        Objects.equals(partial, event.partial);
            }
            else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return Objects.hash(getType(), getKey(), partial);
        }

        @Override
        public String toString() {
            return getType() + " [" + getKey() + "]:" + partial;
        }
    }

    static class Delete<K> extends KeyedDataEvents<K> implements KeyedDataEvent.Delete<K> {

       Delete(K key) {
           super(key);
       }

        @Override
        public boolean equals(Object o) {
            if (o instanceof KeyedDataEvents.Delete <?> event) {
                return Objects.equals(getKey(), event.getKey());
            }
            else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return Objects.hash(getType(), getKey());
        }

        @Override
        public String toString() {
            return getType() + " [" + getKey() + "]";
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

    public static <K> KeyedDataConsumer<K> asKeyedDataConsumer(Consumer<? super KeyedDataEvent<K>> consumer) {

        return new KeyedDataConsumer<K>() {
            @Override
            public void onData(K key, DidoData data) {
                consumer.accept(KeyedDataEvent.complete(key, data));
            }

            @Override
            public void onPartial(K key, PartialData partial) {
                consumer.accept(KeyedDataEvent.partial(key, partial));
            }

            @Override
            public void onDelete(K key) {
                consumer.accept(KeyedDataEvent.delete(key));
            }
        };
    }

    public static <K> Consumer<KeyedDataEvent<K>> fromKeyedDataConsumer(KeyedDataConsumer<? super K> consumer) {

        return event -> {

            switch (event) {
                case KeyedDataEvent.Complete<K> e:
                    consumer.onData(e.getKey(), e.getData());
                    break;
                case KeyedDataEvent.Partial<K> e:
                    consumer.onPartial(e.getKey(), e.getPartial());
                    break;
                case KeyedDataEvent.Delete<K> e:
                    consumer.onDelete(e.getKey());
                    break;
            }
        };
    }
}
