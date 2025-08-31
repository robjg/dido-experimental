package dido.table;

import dido.data.DataSchema;
import dido.data.DidoData;
import dido.flow.DidoPublisher;
import dido.flow.DidoSubscriber;
import dido.flow.DidoSubscription;
import dido.flow.QuietlyCloseable;

import java.util.Map;
import java.util.Set;

public interface DataTable<K extends Comparable<K>> extends DidoPublisher {

    DataSchema getSchema();

    Set<K> keySet();

    Set<Map.Entry<K, DidoData>> entrySet();

    boolean containsKey(K key);

    DidoData get(K key);

    KeyedSubscription tableSubscribe(KeyedSubscriber<K> listener);

    @Override
    default DidoSubscription didoSubscribe(DidoSubscriber subscriber) {
        QuietlyCloseable close = tableSubscribe(new KeyedSubscriber<>() {
            @Override
            public void onData(K key, DidoData data) {
                subscriber.onData(data);
            }

            @Override
            public void onPartial(K key, DidoData data) {
                subscriber.onPartial(data);
            }

            @Override
            public void onDelete(K key, DidoData data) {
                subscriber.onDelete(data);
            }
        });

        return new DidoSubscription() {
            @Override
            public DataSchema getSchema() {
                return DataTable.this.getSchema();
            }

            @Override
            public void close() {
                close.close();
            }
        };
    }
}
