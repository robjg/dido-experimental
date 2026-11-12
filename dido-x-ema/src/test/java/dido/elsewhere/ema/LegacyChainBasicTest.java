package dido.elsewhere.ema;

import com.refinitiv.ema.access.EmaFactory;
import com.refinitiv.ema.access.FieldEntry;
import com.refinitiv.ema.access.FieldList;
import com.refinitiv.ema.rdm.DataDictionary;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

class LegacyChainBasicTest {

    @Test
    void addAndSend() {

        String fieldDictionary = Objects.requireNonNull(
                getClass().getResource("/dictionary/RDMFieldDictionary")).getFile();
        String enumTypeDictionary = Objects.requireNonNull(
                getClass().getResource("/dictionary/enumtype.def")).getFile();

        DataDictionary dictionary = EmaFactory.createDataDictionary();
        dictionary.loadFieldDictionary(fieldDictionary);
        dictionary.loadEnumTypeDictionary(enumTypeDictionary);

        LegacyChainProvider legacyChain = LegacyChainBasic.with()
                .symbolsPerRecord(2)
                .forDictionary(dictionary);

        legacyChain.onKey("APPLE");
        legacyChain.onKey("PEAR");
        legacyChain.onKey("ORANGE");

        FieldList fieldList = legacyChain.chainFor("0#.INDEX");

        List<FieldEntry> entries = DidoEmaTestUtil.loadedEntries(fieldList, dictionary);

        Map<String, String> mapped = entries.stream()
                .collect(Collectors.toMap(
                        FieldEntry::name,
                        entry -> entry.ascii().ascii()));

        System.out.println(mapped);
    }

}