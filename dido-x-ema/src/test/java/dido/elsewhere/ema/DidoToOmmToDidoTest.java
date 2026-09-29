package dido.elsewhere.ema;

import com.refinitiv.ema.access.*;
import com.refinitiv.ema.rdm.DataDictionary;
import com.refinitiv.eta.codec.Codec;
import dido.data.DataSchema;
import dido.data.DidoData;
import dido.data.immutable.ArrayData;
import dido.data.partial.PartialData;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class DidoToOmmToDidoTest {

    @Test
    void create() {

        DidoData data = ArrayData.builder()
                .withString("TRD_RIC", "APP")
                .withString("SRC_SYMB", "Apple")
                .withDouble("BID", 23.4)
                .withInt("BIDSIZE", 10)
                .build();

        String fieldDictionary = Objects.requireNonNull(
                getClass().getResource("/dictionary/RDMFieldDictionary")).getFile();
        String enumTypeDictionary = Objects.requireNonNull(
                getClass().getResource("/dictionary/enumtype.def")).getFile();

        com.refinitiv.eta.transport.Error rsslError = com.refinitiv.eta.transport.TransportFactory.createError();

        com.refinitiv.eta.codec.DataDictionary etaDictionary = com.refinitiv.eta.codec.CodecFactory.createDataDictionary();
        etaDictionary.loadFieldDictionary(fieldDictionary, rsslError);
        etaDictionary.loadEnumTypeDictionary(enumTypeDictionary, rsslError);

        DataDictionary dictionary = EmaFactory.createDataDictionary();
        dictionary.loadFieldDictionary(fieldDictionary);
        dictionary.loadEnumTypeDictionary(enumTypeDictionary);

        DidoToOmm test = DidoToOmm.forSchema(data.getSchema(), dictionary);

        FieldList fieldList = test.apply(data);

        FieldList flDec = JUnitTestConnect.createFieldList();
        JUnitTestConnect.setRsslData(flDec, fieldList, Codec.majorVersion(), Codec.minorVersion(), etaDictionary, null);

        System.out.println("**");
        System.out.println(fieldList.toString(dictionary));
        System.out.println(flDec);

        System.out.println(fieldList.toString(dictionary));
        List<FieldEntry> fieldEntries = new ArrayList<>();
        for (FieldEntry fieldEntry : flDec) {
            // toArray not supported so must do this.
            //noinspection UseBulkOperation
            fieldEntries.add(fieldEntry);
        }

        FieldEntry f0 = fieldEntries.get(0);

        assertThat(f0.name(), is("TRD_RIC"));
        assertThat(f0.loadType(), is(DataType.DataTypes.ASCII));
        assertThat(f0.ascii().ascii(), is("APP"));

        FieldEntry f1 = fieldEntries.get(1);

        assertThat(f1.name(), is("SRC_SYMB"));
        assertThat(f1.loadType(), is(DataType.DataTypes.RMTES));
        assertThat(f1.rmtes().rmtes().toString(), is("Apple"));

        FieldEntry f2 = fieldEntries.get(2);

        assertThat(f2.name(), is("BID"));
        assertThat(f2.loadType(), is(DataType.DataTypes.REAL));
        assertThat(f2.real().asDouble(), is(23.4));

        FieldEntry f3 = fieldEntries.get(3);

        assertThat(f3.name(), is("BIDSIZE"));
        assertThat(f3.loadType(), is(DataType.DataTypes.REAL));
        assertThat(f3.real().mantissa(), is(10L));
        assertThat(f3.real().asDouble(), is(10.0));

        DidoOmmData didoOmmData = DidoOmmData.with()
                .partialSchema(DataSchema.builder()
                        .addNamed("BIDSIZE", int.class)
                        .build())
                .of(dictionary, flDec);


        DidoData copy = didoOmmData.data(flDec);

        assertThat(copy, is(data));
    }

    @Test
    void partial() {

        DidoData data = ArrayData.builder()
                .withString("TRD_RIC", "APP")
                .withString("SRC_SYMB", "Apple")
                .withDouble("BID", 23.4)
                .withInt("BIDSIZE", 10)
                .build();

        String fieldDictionary = Objects.requireNonNull(
                getClass().getResource("/dictionary/RDMFieldDictionary")).getFile();
        String enumTypeDictionary = Objects.requireNonNull(
                getClass().getResource("/dictionary/enumtype.def")).getFile();

        com.refinitiv.eta.transport.Error rsslError = com.refinitiv.eta.transport.TransportFactory.createError();

        com.refinitiv.eta.codec.DataDictionary etaDictionary = com.refinitiv.eta.codec.CodecFactory.createDataDictionary();
        etaDictionary.loadFieldDictionary(fieldDictionary, rsslError);
        etaDictionary.loadEnumTypeDictionary(enumTypeDictionary, rsslError);

        DataDictionary dictionary = EmaFactory.createDataDictionary();
        dictionary.loadFieldDictionary(fieldDictionary);
        dictionary.loadEnumTypeDictionary(enumTypeDictionary);

        DidoToOmm test = DidoToOmm.forSchema(data.getSchema(), dictionary);
        PartialData partial = PartialData.of(data, 3);
        FieldList fieldList = test.partialDataFunction().apply(
                partial);

        FieldList flDec = JUnitTestConnect.createFieldList();
        JUnitTestConnect.setRsslData(flDec, fieldList, Codec.majorVersion(), Codec.minorVersion(), etaDictionary, null);

        System.out.println("**");
        System.out.println(fieldList.toString(dictionary));
        System.out.println(flDec);

        System.out.println(fieldList.toString(dictionary));
        List<FieldEntry> fieldEntries = new ArrayList<>();
        for (FieldEntry fieldEntry : flDec) {
            // toArray not supported so must do this.
            //noinspection UseBulkOperation
            fieldEntries.add(fieldEntry);
        }

        FieldEntry f2 = fieldEntries.getFirst();

        assertThat(f2.name(), is("BID"));
        assertThat(f2.loadType(), is(DataType.DataTypes.REAL));
        assertThat(f2.real().asDouble(), is(23.4));

        DidoOmmData didoOmmData = DidoOmmData.with()
                .schema(data.getSchema())
                .of(dictionary, flDec);

        PartialData copy = didoOmmData.partial(flDec);

        assertThat(copy, is(partial));
    }

}