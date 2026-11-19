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

    private final DictionaryField prevLrField;

    private final DictionaryField nextLrField;

    private final String refCountName;

    private LegacyChainIndex(Settings<?> settings,
                               DataDictionary dictionary,
                            DictionaryField prevLrField,
                            DictionaryField nextLrField) {
        this.dictionary = dictionary;
        this.chainNameFormat = Objects.requireNonNullElse(
                settings.chainNameFormat, DEFAULT_CHAIN_NAME_FORMAT);
        this.firstIndex = settings.firstIndex;
        this.linkFieldFormat = Objects.requireNonNullElse(
                settings.linkFieldFormat, DEFAULT_LINK_FIELD_FORMAT);
        this.firstLink = settings.firstLink == null ? 1 : settings.firstLink;
        this.prevLrField = prevLrField;
        this.nextLrField = nextLrField;
        this.refCountName = Objects.requireNonNullElse(
                settings.refCountName, DEFAULT_REF_COUNT_NAME);
    }

    public static LegacyChainIndex helper(Settings<?> settings, DataDictionary dictionary) {

        DictionaryField prevLrDe = dictionaryEntry(Objects.requireNonNullElse(
                                                  settings.prevLrName, DEFAULT_PREV_LINK_NAME), dictionary);
        DictionaryField nextLrDe = dictionaryEntry(Objects.requireNonNullElse(
                                                settings.nextLrName, DEFAULT_NEXT_LINK_NAME), dictionary);

        return new LegacyChainIndex(settings, dictionary, prevLrDe, nextLrDe);
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
        return this.prevLrField.name();
    }

    public int prevLinkFid() {
        return this.prevLrField.fid();
    }

    public DictionaryField prevLinkField() {
        return this.prevLrField;
    }

    public String nextLinkName() {
        return this.nextLrField.name();
    }

    public int nextLinkFid() {
        return this.nextLrField.fid();
    }

    public DictionaryField nextLinkField() {
        return this.nextLrField;
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

    public static DictionaryField dictionaryEntry(String fieldName, DataDictionary dictionary) {

        DictionaryEntry dictionaryEntry =

         Objects.requireNonNull(
                dictionary.entry(fieldName), "No Field in dictionary " + fieldName);

        return new DictionaryField(fieldName, dictionaryEntry.fid(), dictionaryEntry.rwfType());
    }

    public record DictionaryField(String name, int fid, int rwfType) {

    }
}
