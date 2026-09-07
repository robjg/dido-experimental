package dido.flow;

import dido.data.DidoData;
import dido.data.partial.PartialData;

import java.util.function.Consumer;

public interface DidoPublisher {

    DidoSubscription subscribe(DidoSubscriber subscriber);

    default DidoSubscription subscribe(Consumer<? super DidoData> consumer) {

        return subscribe(new DidoSubscriber() {
            @Override
            public void onData(DidoData data) {
                consumer.accept(data);
            }

            @Override
            public void onPartial(PartialData partial) {

                // doesn't work conceptionally....
            }

            @Override
            public void onDelete(DidoData keyData) {
                // Do we need the concept of deleted data?
            }
        });
    }
}
