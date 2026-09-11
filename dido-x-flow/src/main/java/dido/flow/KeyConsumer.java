package dido.flow;

public interface KeyConsumer<K> {

    void onInsert(K key);

    void onDelete(K key);
}
