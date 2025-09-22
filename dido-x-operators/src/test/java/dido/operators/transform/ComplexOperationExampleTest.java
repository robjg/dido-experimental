package dido.operators.transform;

import dido.data.ArrayData;
import dido.data.DataSchema;
import dido.data.DidoData;
import dido.data.schema.SchemaBuilder;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class ComplexOperationExampleTest {

    static DataSchema schema = ArrayData.schemaBuilder()
            .addNamed("Fruit", String.class)
            .addNamed("Qty", int.class)
            .addNamed("Price", double.class)
            .build();

    DidoData data = DidoData.withSchema(schema)
            .of("Apple", 10, 23.5);


    static class MarkupOperation implements OperationDefinition {

        @Override
        public Runnable prepare(OperationContext context) {

            ValueGetter priceGetter = context.getterNamed("Price");

            ValueSetter markupSetter = context.setterNamed("Markup", double.class);
            ValueSetter amountSetter = context.setterNamed("MarkupAmount", double.class);
            ValueSetter finalSetter = context.setterNamed("FinalPrice", double.class);

            return () -> {
                double price = priceGetter.getDouble();

                double markup;
                if (price > 100.0) {
                    markup = 0.3;
                } else {
                    markup = 0.5;
                }
                markupSetter.setDouble(markup);

                double markupAmount = price * markup;
                amountSetter.setDouble(markupAmount);

                finalSetter.setDouble(price + markupAmount);
            };
        }
    }

    @Test
    void markupExample() {

        DidoTransform transform = OperationTransformBuilder.forSchema(schema)
                .addOp(new MarkupOperation())
                .build();

        DidoData result = transform.apply(data);

        DataSchema expectedSchema = SchemaBuilder.newInstance()
                .addNamed("Markup", double.class)
                .addNamed("MarkupAmount", double.class)
                .addNamed("FinalPrice", double.class)
                .build();

        DidoData expectedData = DidoData.withSchema(expectedSchema)
                .of(0.5, 11.75, 35.25);

        assertThat(result, is(expectedData));
    }
}