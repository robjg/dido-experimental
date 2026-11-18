package dido.elsewhere.ema;

import java.util.function.Supplier;

public class IndexProviderConfigSupplier extends IndexProviderBeanBase
    implements Supplier<LegacyChainProviderConfig> {

    @Override
    public LegacyChainProviderConfig get() {

        return LegacyChainIndexProvider.with()
                .firstIndex(getFirstIndex())
                .chainNameFormat(getChainNameFormat())
                .symbolsPerRecord(getSymbolsPerRecord())
                .firstLink(getFirstLink())
                .prevLrName(getPrevLrName())
                .nextLrName(getNextLrName())
                .linkFieldFormat(getLinkFieldFormat())
                .refCountName(getRefCountName());
    }
}
