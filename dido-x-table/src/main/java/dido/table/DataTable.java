package dido.table;

import dido.data.DataSchema;
import dido.data.DidoData;
import dido.flow.*;
import dido.flow.util.KeyConsumerAdapters;

import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

public interface DataTable<K>
        extends KeyPublisher<K>, KeyedDataPublisher<K> {

    DataSchema getSchema();

    int size();

    Set<K> keySet();

    Set<Map.Entry<K, DidoData>> entrySet();

    boolean containsKey(K key);

    DidoData get(K key);

    default void forEach(BiConsumer<? super K, ? super DidoData> action) {
        entrySet().forEach(entry -> action.accept(entry.getKey(), entry.getValue()));
    }

    @Override
    default QuietlyCloseable subscribeKeys(KeyConsumer<? super K> keyConsumer, SubscriptionOptions options) {
        return subscribeKeyedData(KeyConsumerAdapters.from(keyConsumer), options);
    }
}
