package dido.table;

import dido.data.DidoData;

import java.util.function.Function;

public interface ConcurrentTable<K> extends DataTable<K> {

    <R> R copy(K key, Function<? super DidoData, ? extends R> mapper);
}
