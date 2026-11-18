package dido.elsewhere.ema;

import com.refinitiv.ema.access.EmaFactory;
import com.refinitiv.ema.access.FieldEntry;
import com.refinitiv.ema.access.FieldList;
import com.refinitiv.ema.rdm.DataDictionary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LegacyChainIndexProvider
        implements LegacyChainProvider {

    private static final Logger logger = LoggerFactory.getLogger(LegacyChainIndexProvider.class);

    private final LegacyChainIndex helper;

    private final FieldReady prevLrField;

    private final FieldReady nextLrField;

    private final FieldReady refCountField;

    private final int symbolsPerRecord;

    private final FieldList fieldList = EmaFactory.createFieldList();

    private final List<FieldReady> fieldEntries;

    private final Map<String, ChainRecord> chainRecords = new HashMap<>();

    private ChainRecord currentChain;

    private int lastIndex;

    private LegacyChainIndexProvider(LegacyChainIndex helper,
                                     FieldReady prevLrField,
                                     FieldReady nextLrField,
                                     FieldReady refCountField,
                                     int symbolsPerRecord,
                                     List<FieldReady> fieldEntries
    ) {
        this.helper = helper;
        this.prevLrField = prevLrField;
        this.nextLrField = nextLrField;
        this.refCountField = refCountField;
        this.symbolsPerRecord = symbolsPerRecord;
        this.fieldEntries = fieldEntries;
        this.lastIndex = helper.firstIndex();
    }

    public static class Settings extends LegacyChainIndex.Settings<Settings>
            implements LegacyChainProviderConfig {

        private int symbolsPerRecord;

        @Override
        Settings self() {
            return this;
        }

        public Settings symbolsPerRecord(int symbolsPerRecord) {
            this.symbolsPerRecord = symbolsPerRecord;
            return self();
        }

        @Override
        public LegacyChainIndexProvider forDictionary(DataDictionary dictionary) {

            LegacyChainIndex helper = new LegacyChainIndex(this, dictionary);

            int symbolsPerRecord = this.symbolsPerRecord == 0 ?
                    14 : this.symbolsPerRecord;

            List<FieldReady> fieldEntries = new ArrayList<>();

            for (int i = helper.firstLink(); i <= symbolsPerRecord; i++) {

                fieldEntries.add(fieldReady(helper.linkFieldFid(i)));
            }

            return new LegacyChainIndexProvider(helper,
                    fieldReady(helper.prevLinkFid()),
                    fieldReady(helper.nextLinkFid()),
                    fieldReady(helper.refCountFid()),
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
            currentChain = new ChainRecord(helper.indexChainName(lastIndex++), null);
            logger.debug("Created first chain {}", currentChain.chainName);
        }
        else if (currentChain.size() == symbolsPerRecord) {
            String nextName = helper.indexChainName(lastIndex++);
            currentChain.nextLr = nextName;
            currentChain = new ChainRecord(nextName, currentChain.chainName);
            logger.debug("Created new chain {}", currentChain.chainName);
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
            fieldList.add(refCountField.integer(symbolToFid.size()));

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

        FieldEntry integer(int i) {
            return fieldEntry.intValue(fid, i);
        }
    }

    static FieldReady fieldReady(int fid) {

        return new FieldReady(fid, EmaFactory.createFieldEntry());
    }
}
