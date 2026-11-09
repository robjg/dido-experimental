package dido.elsewhere.ema;

import com.refinitiv.ema.access.FieldEntry;
import com.refinitiv.ema.access.FieldList;
import com.refinitiv.ema.access.JUnitTestConnect;
import com.refinitiv.ema.rdm.DataDictionary;
import com.refinitiv.eta.codec.Codec;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class DidoEmaTestUtil {

    static com.refinitiv.eta.codec.DataDictionary etaDictionary() {

        String fieldDictionary = Objects.requireNonNull(
                DidoEmaTestUtil.class.getResource("/dictionary/RDMFieldDictionary")).getFile();
        String enumTypeDictionary = Objects.requireNonNull(
                DidoEmaTestUtil.class.getResource("/dictionary/enumtype.def")).getFile();

        com.refinitiv.eta.transport.Error rsslError = com.refinitiv.eta.transport.TransportFactory.createError();

        com.refinitiv.eta.codec.DataDictionary etaDictionary = com.refinitiv.eta.codec.CodecFactory.createDataDictionary();
        etaDictionary.loadFieldDictionary(fieldDictionary, rsslError);
        etaDictionary.loadEnumTypeDictionary(enumTypeDictionary, rsslError);

        return etaDictionary;
    }

    static com.refinitiv.eta.codec.DataDictionary etaDictionary(DataDictionary emaDictionary) {

        try {
            Method method = emaDictionary.getClass().getDeclaredMethod("rsslDataDictionary");
            method.setAccessible(true);
            return (com.refinitiv.eta.codec.DataDictionary) method.invoke(emaDictionary);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Loads a field list by converting to binary and back. A field list that
     * has been written locally can't be read locally until this is done.
     * @param fieldList The field list.
     * @param dictionary The dictionary.
     * @return The loaded (encoded/decoded) fieldList.
     */
    public static FieldList load(FieldList fieldList, DataDictionary dictionary) {

        FieldList flDec = JUnitTestConnect.createFieldList();
        JUnitTestConnect.setRsslData(flDec, fieldList, Codec.majorVersion(), Codec.minorVersion(),
                etaDictionary(dictionary), null);

        return flDec;
    }

    public static List<FieldEntry> loadedEntries(FieldList fieldList, DataDictionary dictionary) {

        List<FieldEntry> fieldEntries = new ArrayList<>();
        for (FieldEntry fieldEntry : load(fieldList, dictionary)) {
            // toArray not supported so must do this.
            //noinspection UseBulkOperation
            fieldEntries.add(fieldEntry);
        }

        return fieldEntries;
    }
}
