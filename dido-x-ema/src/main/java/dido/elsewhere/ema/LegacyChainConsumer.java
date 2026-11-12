package dido.elsewhere.ema;

import com.refinitiv.ema.access.FieldList;
import dido.flow.KeyPublisher;

import java.util.function.Consumer;

public interface LegacyChainConsumer extends KeyPublisher<String> {

    void onChain(String chainName,
                 FieldList fieldList,
                 Consumer<? super String> chainSubscriber);
}
