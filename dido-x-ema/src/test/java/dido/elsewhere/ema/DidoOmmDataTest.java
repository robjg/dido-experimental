package dido.elsewhere.ema;

import com.refinitiv.ema.access.*;
import dido.data.DataSchema;
import dido.data.DidoData;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DidoOmmDataTest {

    @Test
    void wrapData() {

        OmmAscii ascii = mock(OmmAscii.class);
        when(ascii.ascii()).thenReturn("Apple");
        FieldEntry entry1 = mock(FieldEntry.class);
        when(entry1.name()).thenReturn("Fruit");
        when(entry1.fieldId()).thenReturn(24);
        when(entry1.loadType()).thenReturn(DataType.DataTypes.ASCII);
        when(entry1.ascii()).thenReturn(ascii);

        FieldEntry entry2 = mock(FieldEntry.class);
        when(entry2.name()).thenReturn("Price");
        when(entry2.fieldId()).thenReturn(42);
        when(entry2.loadType()).thenReturn(DataType.DataTypes.REAL);
        when(entry2.doubleValue()).thenReturn(24.2);

        List<FieldEntry> entries = List.of(entry1, entry2);
        FieldList fieldList = mock(FieldList.class);
        when(fieldList.size()).thenReturn(entries.size());
        when(fieldList.iterator())
                .then(invocationOnMock -> entries.iterator());

        DidoOmmData test = DidoOmmData.of(fieldList);

        DidoData data = test.data(fieldList);

        DataSchema expectedSchema = DataSchema.builder()
                .addNamed("Fruit", String.class)
                .addNamed("Price", double.class)
                .build();

        assertThat(data.getSchema(), is(expectedSchema));

        assertThat(data, is(DidoData.withSchema(expectedSchema)
                .of("Apple", 24.2)));
    }
}