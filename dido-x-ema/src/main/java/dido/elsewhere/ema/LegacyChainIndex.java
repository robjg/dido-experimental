package dido.elsewhere.ema;

import com.refinitiv.ema.rdm.DataDictionary;
import com.refinitiv.ema.rdm.DictionaryEntry;

import java.util.Objects;
import java.util.function.Function;

public class LegacyChainIndex {

    public static final Function<? super Integer, ? extends String> DEFAULT_N_TO_FIELD_FN =
            n -> "LINK_" + n;

    public static final Function<? super Integer, ? extends String> DEFAULT_I_TO_CHAIN_FN =
            n -> n + "#.INDEX";

    private final Function<? super Integer, ? extends String> iToChain;

    private final int firstIndex;

    protected LegacyChainIndex(Settings<?> settings) {
        this.iToChain = Objects.requireNonNullElse(settings.iToCain,
                DEFAULT_I_TO_CHAIN_FN);
        this.firstIndex = settings.firstIndex;
    }

    public abstract static class Settings<B extends Settings<B>>  {

        private int firstIndex;

        private Function<? super Integer, ? extends String> iToCain;

        private String prevLrName;

        private String nextLrName;

        abstract B self();

        public B iToIndex(Function<? super Integer, ? extends String> iToCain) {
            this.iToCain = iToCain;
            return self();
        }

        public B firstIndex(int firstIndex) {
            this.firstIndex = firstIndex;
            return self();
        }

        protected String prevLinkName() {
            return Objects.requireNonNullElse(
                    this.prevLrName, "PREV_LR");
        }

        protected String nextLinkName() {
            return Objects.requireNonNullElse(
                    this.prevLrName, "NEXT_LR");
        }
    }

    public String rootChainName() {
        return iToChain.apply(firstIndex);
    }


    protected static int fieldId(DataDictionary dictionary, String fieldName) {

        DictionaryEntry dictionaryEntry = Objects.requireNonNull(
                dictionary.entry(fieldName), "No Field in dictionary " + fieldName);
        return dictionaryEntry.fid();
    }
}
