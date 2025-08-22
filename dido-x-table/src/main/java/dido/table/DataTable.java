package dido.table;

import dido.data.DataSchema;
import dido.data.DidoData;
import dido.data.partial.PartialData;
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

    QuietlyCloseable tableSubscribe(DataTableSubscriber<K> listener);

    @Override
    default DidoSubscription didoSubscribe(DidoSubscriber subscriber) {
        QuietlyCloseable close = tableSubscribe(new DataTableSubscriber<K>() {
            @Override
            public void onData(K key, DidoData data) {
                subscriber.onData(data);
            }

            @Override
            public void onPartial(K key, PartialData data) {
                subscriber.onPartial(data);
            }

            @Override
            public void onDelete(K key) {
                subscriber.onDelete(get(key));
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
