package dido.elsewhere.ema;

import java.util.function.Supplier;

public class IndexConsumerConfigSupplier extends IndexProviderBeanBase
    implements Supplier<LegacyChainConsumerConfig> {

    @Override
    public LegacyChainConsumerConfig get() {

        return LegacyChainIndexConsumer.with()
                .firstIndex(getFirstIndex())
                .symbolsPerRecord(getSymbolsPerRecord());
    }
}
