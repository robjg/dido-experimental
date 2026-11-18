package dido.elsewhere.ema;

import com.refinitiv.ema.access.FieldList;
import dido.flow.KeyConsumer;

public interface LegacyChainProvider extends KeyConsumer<String> {

    FieldList chainFor(String chainName);

}
