package dido.flow;

/**
 * Publish Dido Data along with a key.
 *
 * @param <K> The key type.
 */
public interface KeyedDataPublisher<K> {

    DidoSubscription subscribe(KeyedDataConsumer<? super K> consumer);
}
