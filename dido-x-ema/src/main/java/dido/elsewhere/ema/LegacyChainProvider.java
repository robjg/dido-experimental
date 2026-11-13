package dido.elsewhere.ema;

import com.refinitiv.ema.access.FieldList;
import dido.flow.KeyConsumer;

public interface LegacyChainProvider extends KeyConsumer<String> {

    FieldList chainFor(String chainName);

    class NoOp implements LegacyChainProvider {

        @Override
        public FieldList chainFor(String chainName) {
            return null;
        }

        @Override
        public void onKey(String key) {

        }

        @Override
        public void onDelete(String key) {

        }
    }
}
