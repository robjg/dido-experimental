package dido.flow;

import dido.data.DidoData;

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
            public void onPartial(DidoData partial) {
                consumer.accept(partial);
            }

            @Override
            public void onDelete(DidoData keyData) {
                // Do we need the concept of deleted data?
            }
        });
    }
}
