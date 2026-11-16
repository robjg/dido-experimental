package dido.elsewhere.ema;

import java.util.function.Supplier;

public class IndexProviderConfigSupplier extends IndexProviderBeanBase
    implements Supplier<LegacyChainProviderConfig> {

    @Override
    public LegacyChainProviderConfig get() {

        return dictionary -> LegacyChainIndexProvider.with()
                .firstIndex(getFirstIndex())
                .symbolsPerRecord(getSymbolsPerRecord())
                .forDictionary(dictionary);

    }
}
