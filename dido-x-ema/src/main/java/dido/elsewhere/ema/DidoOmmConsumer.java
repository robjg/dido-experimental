package dido.elsewhere.ema;

import dido.flow.QuietlyCloseable;

public interface DidoOmmConsumer {

    QuietlyCloseable register(DidoOmmConsumerClient client);

}
