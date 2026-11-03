package dido.flow.util;

import dido.data.DidoData;
import dido.data.partial.PartialData;
import dido.flow.KeyConsumer;
import dido.flow.KeyedDataConsumer;

public class KeyConsumerAdapters {


    public static <K> KeyedDataConsumer<K> from(KeyConsumer<K> keyConsumer) {

        return new KeyedDataConsumer<K>() {

            @Override
            public void onData(K key, DidoData data) {
                keyConsumer.onKey(key);
            }

            @Override
            public void onPartial(K key, PartialData partial) {

            }

            @Override
            public void onDelete(K key) {
                keyConsumer.onDelete(key);
            }
        };

    }

}
