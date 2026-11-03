package dido.table;

import dido.data.DataSchema;
import dido.data.DidoData;
import dido.flow.KeyConsumer;
import dido.flow.KeyPublisher;
import dido.flow.KeyedDataPublisher;
import dido.flow.QuietlyCloseable;
import dido.flow.util.KeyConsumerAdapters;

import java.util.Map;
import java.util.Set;

public interface DataTable<K>
        extends KeyPublisher<K>, KeyedDataPublisher<K> {

    DataSchema getSchema();

    int size();

    Set<K> keySet();

    Set<Map.Entry<K, DidoData>> entrySet();

    boolean containsKey(K key);

    DidoData get(K key);

    @Override
    default QuietlyCloseable subscribeKeys(KeyConsumer<? super K> keyConsumer) {
        return subscribeKeyedData(KeyConsumerAdapters.from(keyConsumer));
    }
}
