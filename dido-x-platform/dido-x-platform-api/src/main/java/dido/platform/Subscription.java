package dido.platform;

import dido.data.DidoData;
import dido.data.partial.PartialData;

import java.util.function.Consumer;

/**
 *
 */
public interface Subscription extends AutoCloseable {

    interface Builder {

        Builder endpoint(String endpoint);

        Builder withFilter(Filter filter);

        Builder withOptions(SubscriptionOptions options);

        Builder onData(Consumer<? super DidoData> consumer);

        Builder onPartial(Consumer<? super PartialData> consumer);

        Builder onDelete(Consumer<? super PartialData> consumer);

        Builder to(Receiver receiver);

        Subscription start();
    }

}
