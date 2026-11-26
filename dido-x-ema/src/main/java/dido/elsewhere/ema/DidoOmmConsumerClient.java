package dido.elsewhere.ema;

import com.refinitiv.ema.access.OmmConsumer;
import com.refinitiv.ema.rdm.DataDictionary;
import dido.flow.QuietlyCloseable;

public interface DidoOmmConsumerClient {

    QuietlyCloseable start(DataDictionary dictionary,
                                  OmmConsumer consumer);

}
