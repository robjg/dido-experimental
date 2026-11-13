package dido.elsewhere.ema;

import com.refinitiv.ema.access.FieldEntry;
import com.refinitiv.ema.access.FieldList;
import com.refinitiv.ema.rdm.DataDictionary;
import dido.flow.KeyConsumer;
import dido.flow.QuietlyCloseable;
import dido.flow.util.KeySubscribers;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

public class LegacyChainIndexConsumer extends LegacyChainIndex
        implements LegacyChainConsumer {

    private final KeySubscribers<String> keySubscribers =  new KeySubscribers<>();

    private final int prevLrFid;

    private final int nextLrFid;

    private final Map<String, Set<String>> chainRecords = new HashMap<>();

    public LegacyChainIndexConsumer(Settings settings,
                                    int prevLrFid, int nextLrFid) {
        super(settings);
        this.prevLrFid = prevLrFid;
        this.nextLrFid = nextLrFid;
    }

    public static class Settings extends LegacyChainIndex.Settings<Settings> {

        @Override
        Settings self() {
            return this;
        }

        public LegacyChainConsumer forDictionary(DataDictionary dictionary) {

            return new LegacyChainIndexConsumer(this,
                    fieldId(dictionary, prevLinkName()),
                    fieldId(dictionary, nextLinkName()));
        }
    }

    public static Settings with() {
        return new Settings();
    }

    public static LegacyChainConsumer forDictionary(DataDictionary dictionary) {
        return with().forDictionary(dictionary);
    }


    @Override
    public void onChain(String chainName,
                        FieldList fieldList,
                        Consumer<? super String> chainSubscriber) {

        Set<String> chainRecord = chainRecords.get(chainName);

        String nextLrName = null;
        Set<String> symbolNames = new HashSet<>();

        for (FieldEntry fieldEntry : fieldList) {

            int fid = fieldEntry.fieldId();
            String value = fieldEntry.ascii().ascii();

            if (fid != prevLrFid) {
                if (fid == nextLrFid){
                    nextLrName = value;
                }
                else {
                    symbolNames.add(value);
                }
            }
        }

        if (nextLrName != null && !chainRecords.containsKey(nextLrName)) {
            chainSubscriber.accept(nextLrName);
        }

        Set<String> newSymbols;
        Set<String> deletedSymbols;

        if (chainRecord == null) {

            newSymbols = symbolNames;
            deletedSymbols = Set.of();
        }
        else {

            newSymbols = new HashSet<>(symbolNames);
            newSymbols.removeAll(chainRecord);
            deletedSymbols = chainRecord;
            deletedSymbols.removeAll(symbolNames);
        }

        chainRecords.put(chainName, symbolNames);

        for (String deletedSymbol : deletedSymbols) {
            keySubscribers.onDelete(deletedSymbol);
        }

        for (String addedSymbol : newSymbols) {
            keySubscribers.onKey(addedSymbol);
        }

    }

    @Override
    public QuietlyCloseable subscribeKeys(KeyConsumer<? super String> keyConsumer) {
        for  (Set<String> chainRecord : chainRecords.values()) {
            for (String symbolName : chainRecord) {
                keySubscribers.onKey(symbolName);
            }
        }
        return keySubscribers.addKeySubscriber(keyConsumer);
    }

}
