package dido.elsewhere.ema;

import com.refinitiv.ema.access.EmaFactory;
import com.refinitiv.ema.access.FieldEntry;
import com.refinitiv.ema.access.FieldList;
import com.refinitiv.ema.rdm.DataDictionary;
import dido.flow.KeyConsumer;
import dido.flow.QuietlyCloseable;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

class LegacyChainIndexTest {

    @Test
    void addAndSend() {

        String fieldDictionary = Objects.requireNonNull(
                getClass().getResource("/dictionary/RDMFieldDictionary")).getFile();
        String enumTypeDictionary = Objects.requireNonNull(
                getClass().getResource("/dictionary/enumtype.def")).getFile();

        DataDictionary dictionary = EmaFactory.createDataDictionary();
        dictionary.loadFieldDictionary(fieldDictionary);
        dictionary.loadEnumTypeDictionary(enumTypeDictionary);

        LegacyChainProvider legacyChain = LegacyChainIndexProvider.with()
                .symbolsPerRecord(2)
                .forDictionary(dictionary);

        legacyChain.onKey("APPLE");
        legacyChain.onKey("PEAR");
        legacyChain.onKey("ORANGE");

        Function<String, FieldList> loadedFieldList =
                chainName -> {

                    FieldList fieldList = legacyChain.chainFor(chainName);

                    return DidoEmaTestUtil.loadedFieldList(fieldList, dictionary);
                };

        List<FieldEntry> entries = DidoEmaTestUtil.entriesFor(
                loadedFieldList.apply("0#.INDEX"));

        Map<String, String> mapped = entries.stream()
                .collect(Collectors.toMap(
                        FieldEntry::name,
                        entry -> entry.ascii().ascii()));

        System.out.println(mapped);

        LegacyChainConsumer chainConsumer = LegacyChainIndexConsumer
                .forDictionary(dictionary);

        Set<String> results = new HashSet<>();

        try (QuietlyCloseable closeable = chainConsumer.subscribeKeys(new KeyConsumer<String>() {
            @Override
            public void onKey(String key) {
                results.add(key);
            }

            @Override
            public void onDelete(String key) {
                results.remove(key);
            }
        })) {

            AtomicReference<Consumer<String>> consumerRef = new AtomicReference<>();
            Consumer<String> consumer = chainName ->
                    chainConsumer.onChain(chainName, loadedFieldList.apply(chainName),
                    consumerRef.get());
            consumerRef.set(consumer);

            consumer.accept(chainConsumer.rootChainName());
        }

        assertThat(results, containsInAnyOrder("APPLE", "PEAR", "ORANGE"));
    }
}