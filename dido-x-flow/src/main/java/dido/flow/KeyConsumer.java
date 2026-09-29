package dido.flow;

public interface KeyConsumer<K> {

    void onAvailable(K key);

    void onRemoved(K key);
}
