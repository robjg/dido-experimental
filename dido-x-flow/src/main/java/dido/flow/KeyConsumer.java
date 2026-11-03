package dido.flow;

public interface KeyConsumer<K> {

    void onKey(K key);

    void onDelete(K key);
}
