package dido.elsewhere.ema;

import com.refinitiv.ema.access.EmaFactory;
import com.refinitiv.ema.access.FieldEntry;
import com.refinitiv.ema.access.FieldList;
import com.refinitiv.ema.rdm.DataDictionary;

import java.util.*;
import java.util.function.Function;

public class LegacyChainIndexProvider extends LegacyChainIndex
        implements LegacyChainProvider {

    public final Function<? super Integer, ? extends String> iToChain = DEFAULT_I_TO_CHAIN_FN;

    private final FieldReady prevLrField;

    private final FieldReady nextLrField;

    private final int symbolsPerRecord;

    private final FieldList fieldList = EmaFactory.createFieldList();

    private final List<FieldReady> fieldEntries;

    private final Map<String, ChainRecord> chainRecords = new HashMap<>();

    ChainRecord currentChain;

    int lastIndex = 0;

    private LegacyChainIndexProvider(Settings settings,
                                     FieldReady prevLrField,
                                     FieldReady nextLrField,
                                     int symbolsPerRecord,
                                     List<FieldReady> fieldEntries
    ) {
        super(settings);
        this.prevLrField = prevLrField;
        this.nextLrField = nextLrField;
        this.symbolsPerRecord = symbolsPerRecord;
        this.fieldEntries = fieldEntries;
    }

    public static class Settings extends LegacyChainIndex.Settings<Settings> {

        private int symbolsPerRecord;

        private Function<? super Integer, ? extends String> nToField;

        Settings self() {
            return this;
        }


        public Settings symbolsPerRecord(int symbolsPerRecord) {
            this.symbolsPerRecord = symbolsPerRecord;
            return this;
        }

        public LegacyChainIndexProvider forDictionary(DataDictionary dictionary) {

            FieldReady prevLrField = field(dictionary, prevLinkName());

            FieldReady nextLrField = field(dictionary, nextLinkName());

            Function<? super Integer, ? extends String> nToField =
                    Objects.requireNonNullElse(this.nToField, DEFAULT_N_TO_FIELD_FN);

            int symbolsPerRecord = this.symbolsPerRecord == 0 ?
                    17 : this.symbolsPerRecord;

            List<FieldReady> fieldEntries = new ArrayList<>();

            for (int i = 1; i <= symbolsPerRecord; i++) {

                String fieldName = nToField.apply(i);
                fieldEntries.add(field(dictionary, fieldName));
            }

            return new LegacyChainIndexProvider(this,
                    prevLrField,
                    nextLrField,
                    symbolsPerRecord,
                    fieldEntries);
        }
    }

    public static Settings with() {
        return new Settings();
    }

    public static LegacyChainProvider forDictionary(DataDictionary dictionary) {
        return with().forDictionary(dictionary);
    }

    @Override
    public void onKey(String key) {

        if (this.currentChain == null) {
            currentChain = new ChainRecord(iToChain.apply(lastIndex++), null);
        }
        else if (currentChain.size() == symbolsPerRecord) {
            String nextName = iToChain.apply(lastIndex++);
            currentChain.nextLr = nextName;
            currentChain = new ChainRecord(nextName, currentChain.chainName);
        }

        currentChain.addSymbol(key);
    }

    @Override
    public void onDelete(String key) {

    }

    @Override
    public FieldList chainFor(String chainName) {
        ChainRecord chainRecord = chainRecords.get(chainName);
        if (chainRecord == null) {
            return null;
        }
        else {
            return chainRecord.toFieldList();
        }
    }

    class ChainRecord {

        private final String chainName;

        private final String prevLr;

        private String nextLr;

        private final Map<String, FieldReady> symbolToFid = new HashMap<>();

        private int nextN = 0;

        ChainRecord(String chainName, String prevLr) {
            this.chainName = chainName;
            this.prevLr = prevLr;
            chainRecords.put(chainName, this);
        }

        void addSymbol(String symbol) {
            symbolToFid.put(symbol, fieldEntries.get(nextN++));
        }

        FieldList toFieldList() {

            fieldList.clear();
            if (prevLr != null) {
                fieldList.add(prevLrField.ascii(prevLr));
            }
            if (nextLr != null) {
                fieldList.add(nextLrField.ascii(nextLr));
            }

            for (Map.Entry<String, FieldReady> entry : symbolToFid.entrySet()) {

                fieldList.add(entry.getValue().ascii(entry.getKey()));
            }

            return fieldList;
        }

        int size() {
            return symbolToFid.size();
        }
    }

    /** Wraps a Fid and FieldEntry ready to be populated and sent */
    record FieldReady(int fid, FieldEntry fieldEntry) {

        FieldEntry ascii(String ascii) {
            return fieldEntry.ascii(fid, ascii);
        }
    }

    static FieldReady field(DataDictionary dictionary, String fieldName) {

        return new FieldReady(fieldId(dictionary, fieldName), EmaFactory.createFieldEntry());
    }
}
