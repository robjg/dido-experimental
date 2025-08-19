package dido.operators.transform;

import dido.data.ArrayData;
import dido.data.DataSchema;
import dido.data.DidoData;
import dido.data.SchemaBuilder;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class OperationsTest {

    static DataSchema schema = ArrayData.schemaBuilder()
            .addNamed("Fruit", String.class)
            .addNamed("Qty", int.class)
            .addNamed("Price", double.class)
            .build();

    DidoData data = ArrayData.withSchema(schema)
            .of("Apple", 10, 23.5);


    static class CopyDef implements OperationDefinition {

        private final String from;

        private final String to;

        CopyDef(String from, String to) {
            this.from = from;
            this.to = to;
        }

        @Override
        public Runnable prepare(OperationContext context) {
            ValueGetter getter = context.getterNamed(from);
            ValueSetter setter = context.setterNamed(to, getter.getType());
            return () -> setter.set(getter.get());
        }
    }

    @Test
    void copy() {

        DidoTransform transform = OperationTransformBuilder.forSchema(schema)
                .addOp(new CopyDef("Price", "OldPrice"))
                .create();

        DidoData result = transform.apply(data);

        DataSchema expectedSchema = SchemaBuilder.newInstance()
                .addNamed("OldPrice", double.class)
                .build();

        assertThat(result.getSchema(), is(expectedSchema));

        DidoData expectedData = DidoData.withSchema(expectedSchema)
                .of(23.5);

        assertThat(result, is(expectedData));

    }

}
