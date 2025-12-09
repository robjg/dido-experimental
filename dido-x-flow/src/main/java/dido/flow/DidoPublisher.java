package dido.flow;

import dido.data.DidoData;
import dido.data.partial.PartialUpdate;

import java.util.function.Consumer;

public interface DidoPublisher {

    DidoSubscription didoSubscribe(DidoSubscriber subscriber);

    default DidoSubscription didoSubscribe(Consumer<? super DidoData> consumer) {

        return didoSubscribe(new DidoSubscriber() {
            @Override
            public void onData(DidoData data) {
                consumer.accept(data);
            }

            @Override
            public void onPartial(PartialUpdate partial) {

                // doesn't work conceptionally....
            }

            @Override
            public void onDelete(DidoData keyData) {
                // Do we need the concept of deleted data?
            }
        });
    }
}
