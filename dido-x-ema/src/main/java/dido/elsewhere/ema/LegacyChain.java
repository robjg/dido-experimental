package dido.elsewhere.ema;

import com.refinitiv.ema.access.FieldList;
import dido.flow.KeyConsumer;

public interface LegacyChain extends KeyConsumer<String> {

    FieldList chainFor(String chainName);

    class NoOp implements LegacyChain {
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
