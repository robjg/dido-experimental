package dido.table.beans;

import dido.data.DidoData;
import dido.table.DataTable;
import org.junit.jupiter.api.Test;
import org.oddjob.Oddjob;
import org.oddjob.OddjobLookup;
import org.oddjob.Resettable;
import org.oddjob.arooa.convert.ArooaConversionException;
import org.oddjob.arooa.utils.TypeToken;

import java.io.File;
import java.util.Objects;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class BasicTableServiceTest {

    @Test
    void populate() throws ArooaConversionException {

        Oddjob oddjob = new Oddjob();
        oddjob.setFile(new File(Objects.requireNonNull(
                getClass().getResource("/examples/DataTableExample.xml")).getFile()));

        oddjob.run();

        assertThat(oddjob.lastStateEvent().getState().isComplete(), is(true));

        OddjobLookup lookup = new OddjobLookup(oddjob);

        DataTable<String> table = lookup.lookup("table.table",
                new TypeToken<DataTable<String>>() {}.getType());

        assertThat(table.size(), is(3));

        DidoData expected = DidoData.withSchema(table.getSchema())
                .of("Apple", 5, 27.2);

        DidoData actual = table.get("Apple");

        assertThat(actual, is(expected));

        Resettable resettable = lookup.lookup("bus", Resettable.class);

        resettable.hardReset();

        oddjob.run();

        assertThat(oddjob.lastStateEvent().getState().isComplete(), is(true));

        oddjob.destroy();
    }


}