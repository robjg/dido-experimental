package dido.elsewhere.ema;

import com.refinitiv.ema.rdm.DataDictionary;

public interface LegacyChainConsumerConfig {

    LegacyChainConsumer forDictionary(DataDictionary dictionary);
}
