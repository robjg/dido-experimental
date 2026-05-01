package dido.elsewhere.ema;

import com.refinitiv.ema.access.DataType;
import com.refinitiv.ema.access.FieldEntry;
import com.refinitiv.ema.access.FieldList;
import dido.data.DidoData;
import dido.data.immutable.ArrayData;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

class DidoToOmmTest {

    @Test
    @Disabled
    void create() {

        DidoData data = ArrayData.builder()
                .withString("Fruit", "Apple")
                .withDouble("Price", 23.4)
                .build();

        DidoToOmm test = DidoToOmm.forSchema(data.getSchema());

        FieldList fieldList = test.apply(data);

        List<FieldEntry> fieldEntries = new ArrayList<>();
        for (FieldEntry entry : fieldList) {
            fieldEntries.add(entry);
        }

        FieldEntry f0 = fieldEntries.get(0);
        MatcherAssert.assertThat(f0.loadType(), Matchers.is(DataType.DataTypes.ASCII));

        System.out.println(fieldList);

        DidoData copy = DidoOmmData.of(fieldList).data(fieldList);

        MatcherAssert.assertThat(copy, Matchers.is(data));
    }
}