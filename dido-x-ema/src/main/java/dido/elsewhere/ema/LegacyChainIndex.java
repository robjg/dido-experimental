package dido.elsewhere.ema;

import com.refinitiv.ema.rdm.DataDictionary;
import com.refinitiv.ema.rdm.DictionaryEntry;

import java.util.Objects;

public class LegacyChainIndex {

    public static final String DEFAULT_PREV_LINK_NAME = "PREV_LR";

    public static final String DEFAULT_NEXT_LINK_NAME = "NEXT_LR";

    public static final String DEFAULT_REF_COUNT_NAME = "REF_COUNT";

    public static final String DEFAULT_LINK_FIELD_FORMAT = "LINK_%d";

    public static final String DEFAULT_CHAIN_NAME_FORMAT = "%d#.INDEX";

    private final DataDictionary dictionary;

    private final String chainNameFormat;

    private final int firstIndex;

    private final String linkFieldFormat;

    private final int firstLink;

    private final String prevLrName;

    private final String nextLrName;

    private final String refCountName;

    public LegacyChainIndex(Settings<?> settings,
                               DataDictionary dictionary) {
        this.dictionary = dictionary;
        this.chainNameFormat = Objects.requireNonNullElse(
                settings.chainNameFormat, DEFAULT_CHAIN_NAME_FORMAT);
        this.firstIndex = settings.firstIndex;
        this.linkFieldFormat = Objects.requireNonNullElse(
                settings.linkFieldFormat, DEFAULT_LINK_FIELD_FORMAT);
        this.firstLink = settings.firstLink == null ? 1 : settings.firstLink;
        this.prevLrName = Objects.requireNonNullElse(
                settings.prevLrName, DEFAULT_PREV_LINK_NAME);
        this.nextLrName = Objects.requireNonNullElse(
                settings.nextLrName, DEFAULT_NEXT_LINK_NAME);
        this.refCountName = Objects.requireNonNullElse(
                settings.refCountName, DEFAULT_REF_COUNT_NAME);
    }

    public abstract static class Settings<B extends Settings<B>>  {

        private int firstIndex;

        private Integer firstLink;

        private String chainNameFormat;

        private String prevLrName;

        private String nextLrName;

        private String refCountName;

        private String linkFieldFormat;

        abstract B self();

        public B firstIndex(int firstIndex) {
            this.firstIndex = firstIndex;
            return self();
        }

        public B chainNameFormat(String chainNameFormat) {
            this.chainNameFormat = chainNameFormat;
            return self();
        }

        public B firstLink(Integer firstLink) {
            this.firstLink = firstLink;
            return self();
        }

        public B prevLrName(String prevLrName) {
            this.prevLrName = prevLrName;
            return self();
        }

        public B nextLrName(String nextLrName) {
            this.nextLrName = nextLrName;
            return self();
        }

        public B refCountName(String refCountName) {
            this.refCountName = refCountName;
            return self();
        }

        public B linkFieldFormat(String linkFieldFormat) {
            this.linkFieldFormat = linkFieldFormat;
            return self();
        }

    }

    public int firstIndex() {
        return firstIndex;
    }

    public  String indexChainName(int index) {
        return String.format(this.chainNameFormat, index);
    }

    public  String rootChainName() {
        return indexChainName(firstIndex);
    }

    public int firstLink() {
        return firstLink;
    }

    public String prevLinkName() {
        return this.prevLrName;
    }

    public int prevLinkFid() {
        return fieldId(prevLrName);
    }

    public String nextLinkName() {
        return this.nextLrName;
    }

    public int nextLinkFid() {
        return fieldId(nextLrName);
    }

    public String refCountName() {
        return this.refCountName;
    }

    public int refCountFid() {
        return fieldId(refCountName);
    }

    protected String linkFieldName(int linkNum) {
        return String.format(linkFieldFormat, linkNum);
    }

    protected int linkFieldFid(int linkNum) {
        return fieldId(linkFieldName(linkNum));
    }


    public int fieldId(String fieldName) {

        DictionaryEntry dictionaryEntry = Objects.requireNonNull(
                dictionary.entry(fieldName), "No Field in dictionary " + fieldName);
        return dictionaryEntry.fid();
    }
}
