package dido.flow.util;

import dido.data.DidoData;
import dido.data.partial.PartialData;
import dido.flow.KeyedDataEvent;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class KeyedDataEventsTest {

    @Test
    void testKeyedDataEvent() {

        DidoData data = DidoData.of();

        KeyedDataEvent.Complete <String> complete = KeyedDataEvent.complete("Foo", data);

        assertThat(complete.getType(), is(KeyedDataEvent.Type.COMPLETE));
        assertThat(complete.getKey(), is("Foo"));
        assertThat(complete.getData(), is(data));

        KeyedDataEvent.Partial <String> partial = KeyedDataEvent.partial("Foo", PartialData.of(data));

        assertThat(partial.getType(), is(KeyedDataEvent.Type.PARTIAL));
        assertThat(partial.getKey(), is("Foo"));
        assertThat(partial.getPartial().getData(), is(data));

        KeyedDataEvent.Delete <String> delete = KeyedDataEvent.delete("Foo");

        assertThat(delete.getType(), is(KeyedDataEvent.Type.DELETE));
        assertThat(delete.getKey(), is("Foo"));
    }

}