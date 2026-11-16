package dido.elsewhere.ema;

import com.refinitiv.ema.rdm.DataDictionary;

public interface LegacyChainProviderConfig {

    LegacyChainProvider forDictionary(DataDictionary dictionary);
}
