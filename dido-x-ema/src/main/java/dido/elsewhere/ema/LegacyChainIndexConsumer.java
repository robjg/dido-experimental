package dido.elsewhere.ema;

import com.refinitiv.ema.access.FieldEntry;
import com.refinitiv.ema.access.FieldList;
import com.refinitiv.ema.rdm.DataDictionary;
import dido.flow.KeyConsumer;
import dido.flow.QuietlyCloseable;
import dido.flow.SubscriptionOptions;
import dido.flow.util.KeySubscribers;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

public class LegacyChainIndexConsumer
        implements LegacyChainConsumer {

    private final KeySubscribers<String> keySubscribers = new KeySubscribers<>();

    private final LegacyChainIndex helper;

    private final DataDictionary dictionary;

    private final int prevLrFid;

    private final int nextLrFid;

    private final int refCountFid;

    private final Map<Integer, Integer> linkFids = new HashMap<>();

    private final Map<String, Set<String>> chainRecords = new HashMap<>();

    public LegacyChainIndexConsumer(LegacyChainIndex helper,
                                    DataDictionary dictionary,
                                    int prevLrFid,
                                    int nextLrFid,
                                    int refCountFid) {
        this.helper = helper;
        this.dictionary = dictionary;
        this.prevLrFid = prevLrFid;
        this.nextLrFid = nextLrFid;
        this.refCountFid = refCountFid;
    }

    public static class Settings extends LegacyChainIndex.Settings<Settings>
            implements LegacyChainConsumerConfig {

        @Override
        Settings self() {
            return this;
        }

        @Override
        public LegacyChainConsumer forDictionary(DataDictionary dictionary) {

            LegacyChainIndex helper = new LegacyChainIndex(this, dictionary);

            return new LegacyChainIndexConsumer(helper,
                    dictionary,
                    helper.prevLinkFid(),
                    helper.nextLinkFid(),
                    helper.refCountFid());
        }
    }

    public static Settings with() {
        return new Settings();
    }

    public static LegacyChainConsumer forDictionary(DataDictionary dictionary) {
        return with().forDictionary(dictionary);
    }

    @Override
    public String rootChainName() {
        return helper.rootChainName();
    }

    @Override
    public void onChain(String chainName,
                        FieldList fieldList,
                        Consumer<? super String> chainSubscriber) {

        Set<String> chainRecord = chainRecords.get(chainName);

        Set<String> symbolNames = new HashSet<>();

        Map<Integer, FieldEntry> fields = new HashMap<>();
        for (FieldEntry fieldEntry : fieldList) {
            fields.put(fieldEntry.fieldId(), fieldEntry);
        }

        int firstLink = helper.firstLink();
        long refCount = fields.get(refCountFid).uintValue();
        if (refCount > linkFids.size()) {
            for (int i = Math.max(firstLink, linkFids.size()); i <= refCount; i++) {
                linkFids.put(i, helper.fieldId(helper.linkFieldName(i)));
            }
        }

        for (int i = firstLink; i <= refCount; i++) {
            int fid = linkFids.get(i);
            FieldEntry fieldEntry = fields.get(fid);
            if (fieldEntry != null) {
                symbolNames.add(fieldEntry.ascii().ascii());
            }
        }

        FieldEntry nextLrEntry = fields.get(nextLrFid);
        String nextLrName = nextLrEntry == null ? null : nextLrEntry.ascii().ascii();

        if (nextLrName != null && !chainRecords.containsKey(nextLrName)) {
            chainSubscriber.accept(nextLrName);
        }

        Set<String> newSymbols;
        Set<String> deletedSymbols;

        if (chainRecord == null) {

            newSymbols = symbolNames;
            deletedSymbols = Set.of();
        } else {

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
    public QuietlyCloseable subscribeKeys(KeyConsumer<? super String> keyConsumer, SubscriptionOptions options) {
        if (options.snapshot()) {
            for (Set<String> chainRecord : chainRecords.values()) {
                for (String symbolName : chainRecord) {
                    keySubscribers.onKey(symbolName);
                }
            }
            keyConsumer.snapshotComplete();
        }
        if (options.delta()) {
            return keySubscribers.addKeySubscriber(keyConsumer);
        } else {
            return () -> {
            };
        }
    }

}
