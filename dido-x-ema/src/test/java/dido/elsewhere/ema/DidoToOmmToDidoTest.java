package dido.elsewhere.ema;

import com.refinitiv.ema.access.Data;
import com.refinitiv.ema.access.DataType;
import com.refinitiv.ema.access.FieldEntry;
import com.refinitiv.ema.access.FieldList;
import com.refinitiv.ema.rdm.DataDictionary;
import dido.data.DataSchema;
import dido.data.DidoData;
import dido.data.immutable.ArrayData;
import dido.data.partial.PartialData;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class DidoToOmmToDidoTest {

    private static final Logger logger = LoggerFactory.getLogger(DidoToOmmToDidoTest.class);

    @Test
    void create() {

        DidoData data = ArrayData.builder()
                .withString("TRD_RIC", "APP")
                .withString("SRC_SYMB", "Apple")
                .withDouble("BID", 23.4)
                .withInt("BIDSIZE", 10)
                .build();

        DataDictionary dictionary = DidoEmaTestUtil.stanadardDictionary();

        DidoToOmm test = DidoToOmm.forSchema(data.getSchema(), dictionary);

        FieldList fieldList = test.apply(data);

        FieldList flDec = DidoEmaTestUtil.loadedFieldList(fieldList, dictionary);

        logger.info("** Field List:");
        logger.info(fieldList.toString(dictionary));

        List<FieldEntry> fieldEntries = DidoEmaTestUtil.fieldEntriesFrom(flDec);

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

        // Back to DidoData

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

        DataDictionary dictionary = DidoEmaTestUtil.stanadardDictionary();

        DidoToOmm test = DidoToOmm.forSchema(data.getSchema(), dictionary);
        PartialData partial = PartialData.of(data, 3);
        FieldList fieldList = test.partialDataFunction().apply(
                partial);

        FieldList flDec = DidoEmaTestUtil.loadedFieldList(fieldList, dictionary);

        logger.info("** Partial Field List:");
        logger.info(fieldList.toString(dictionary));

        List<FieldEntry> fieldEntries = DidoEmaTestUtil.fieldEntriesFrom(flDec);

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

    @Test
    void blank() {

        DataSchema schema = DataSchema.builder()
                .addNamed("TRD_RIC", String.class)
                .addNamed("SRC_SYMB", String.class)
                .addNamed("BID", Double.class)
                .addNamed("BIDSIZE", Integer.class)
                .build();

        DidoData data = ArrayData.withSchema(schema).of(null, null, null, null);

        DataDictionary dictionary = DidoEmaTestUtil.stanadardDictionary();

        DidoToOmm test = DidoToOmm.forSchema(data.getSchema(), dictionary);

        FieldList fieldList = test.apply(data);

        FieldList flDec = DidoEmaTestUtil.loadedFieldList(fieldList, dictionary);

        logger.info("** Blank Field List:");
        logger.info(fieldList.toString(dictionary));

        List<FieldEntry> fieldEntries = DidoEmaTestUtil.fieldEntriesFrom(flDec);

        FieldEntry f0 = fieldEntries.get(0);

        assertThat(f0.name(), is("TRD_RIC"));
        assertThat(f0.code(), is(Data.DataCode.BLANK));

        FieldEntry f1 = fieldEntries.get(1);

        assertThat(f1.name(), is("SRC_SYMB"));
        assertThat(f1.code(), is(Data.DataCode.BLANK));

        FieldEntry f2 = fieldEntries.get(2);

        assertThat(f2.name(), is("BID"));
        assertThat(f2.code(), is(Data.DataCode.BLANK));

        FieldEntry f3 = fieldEntries.get(3);

        assertThat(f3.name(), is("BIDSIZE"));
        assertThat(f3.code(), is(Data.DataCode.BLANK));

        // Back to DidoData

        DidoOmmData didoOmmData = DidoOmmData.with()
                .partialSchema(DataSchema.builder()
                        .addNamed("BIDSIZE", int.class)
                        .build())
                .of(dictionary, flDec);

        DidoData copy = didoOmmData.data(flDec);

        assertThat(copy, is(data));
    }
}