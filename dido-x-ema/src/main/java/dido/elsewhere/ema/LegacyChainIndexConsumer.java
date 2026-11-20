package dido.elsewhere.ema;

import com.refinitiv.ema.access.FieldEntry;
import com.refinitiv.ema.access.FieldList;
import com.refinitiv.ema.rdm.DataDictionary;
import dido.flow.KeyConsumer;
import dido.flow.QuietlyCloseable;
import dido.flow.SubscriptionOptions;
import dido.flow.util.KeySubscribers;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;

public class LegacyChainIndexConsumer
        implements LegacyChainConsumer {

    private final KeySubscribers<String> keySubscribers = new KeySubscribers<>();

    private final LegacyChainIndex helper;

    private final StringField prevLrField;

    private final StringField nextLrField;

    private final int refCountFid;

    private final Map<Integer, StringField> linkFids = new HashMap<>();

    private final Map<String, Set<String>> chainRecords = new HashMap<>();

    LegacyChainIndexConsumer(LegacyChainIndex helper,
                             StringField prevLrField,
                             StringField nextLrField,
                             int refCountFid) {
        this.helper = helper;
        this.prevLrField = prevLrField;
        this.nextLrField = nextLrField;
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

            LegacyChainIndex helper = LegacyChainIndex.helper(this, dictionary);

            StringField prevLinkField = stringFieldFor(helper.prevLinkField());
            StringField nextLinkField = stringFieldFor(helper.nextLinkField());

            return new LegacyChainIndexConsumer(helper,
                    prevLinkField,
                    nextLinkField,
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
                linkFids.put(i, stringFieldFor(helper.linkField(i)));
            }
        }

        for (int i = firstLink; i <= refCount; i++) {
            StringField field = linkFids.get(i);
            FieldEntry fieldEntry = fields.get(field.fid());
            if (fieldEntry != null) {

                symbolNames.add(field.toString(fieldEntry));
            }
        }

        FieldEntry nextLrEntry = fields.get(nextLrField.fid);
        String nextLrChain = nextLrEntry == null ? null : nextLrField.toString(nextLrEntry);

        if (nextLrChain != null && !chainRecords.containsKey(nextLrChain)) {
            chainSubscriber.accept(nextLrChain);
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

    record StringField(int fid, Function<FieldEntry, String> extractor) {

        String toString(FieldEntry fieldEntry) {
            return extractor.apply(fieldEntry);
        }
    }

    static StringField stringFieldFor(LegacyChainIndex.DictionaryField de) {

        return new StringField(de.fid(), Objects.requireNonNull(
                EmaToJava.stringExtractor(de.rwfType()), "No String Extractor for dictionary entry " + de));
    }
}
