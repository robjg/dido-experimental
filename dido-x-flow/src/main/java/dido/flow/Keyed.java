package dido.flow;

import java.util.Set;

public interface Keyed<K> {

    int size();

    Set<K> keySet();

    QuietlyCloseable keySubscribe(KeyConsumer<? super K> keyConsumer);
}
