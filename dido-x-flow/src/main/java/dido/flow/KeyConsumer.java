package dido.flow;

public interface KeyConsumer<K> {

    default void snapshotComplete() {}

    void onKey(K key);

    void onDelete(K key);
}
