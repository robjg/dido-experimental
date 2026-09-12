package dido.flow;

import dido.data.DidoData;
import dido.data.partial.PartialData;

import java.util.function.Consumer;

public interface DidoPublisher {

    DidoSubscription subscribe(DidoDataConsumer subscriber);

}
