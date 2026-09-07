package dido.table;

import dido.data.DataSchema;
import dido.data.DidoData;
import dido.flow.KeyedDidoPublisher;

import java.util.Map;
import java.util.Set;

public interface DataTable<K extends Comparable<K>> extends KeyedDidoPublisher<K> {

    DataSchema getSchema();

    int size();

    Set<K> keySet();

    Set<Map.Entry<K, DidoData>> entrySet();

    boolean containsKey(K key);

    DidoData get(K key);
}
