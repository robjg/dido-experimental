package dido.table.internal;

import dido.data.DataSchema;
import dido.data.DidoData;
import dido.data.schema.SchemaBuilder;
import dido.flow.DidoSubscriber;
import dido.flow.util.SubscriberUtil;
import dido.operators.transform.BasicOperations;
import dido.operators.transform.ValueGetter;
import dido.operators.transform.ValueSetter;
import dido.table.LiveRow;
import dido.table.LiveTable;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class LiveTableBasicTest {

    @Test
    void insertTwoRows() {

        DataSchema schema = DataSchema.builder()
                .addNamed("Id", int.class)
                .addNamed("Fruit", String.class)
                .build();

        LiveTable<Integer> table = LiveTableBasic.<Integer>forSchema(schema).create();

        DidoSubscriber didoSubscriber = SubscriberUtil.didoSubscriberFrom(table, table.getSchema());

        DidoData.withSchema(schema).many()
                .of(5, "Apple")
                .of(8, "Pear")
                .of(3, "Banana")
                .toList().forEach(didoSubscriber::onData);

        LiveRow row = table.getRow(5);

        assertThat(row.getValueNamed("Fruit").getString(), is("Apple"));
        table.close();
    }

    @Test
    void simpleOp() {

        DataSchema schema = DataSchema.builder()
                .addNamed("Id", int.class)
                .addNamed("Fruit", String.class)
                .addNamed("Price", String.class)
                .build();

        LiveTable<Integer> table = LiveTableBasic.<Integer>forSchema(schema)
                .addOperation(BasicOperations.map()
                        .from("Price")
                        .to("Tax")
                        .with().doubleOp(price -> price * 0.1))
                .addOperation(opContext -> {
                    ValueGetter price = opContext.getterNamed("Price");
                    ValueGetter tax = opContext.getterNamed("Tax");
                    ValueSetter totalPrice = opContext.setterNamed("TotalPrice", double.class);
                    return () -> totalPrice.setDouble(price.getDouble() + tax.getDouble());
                })
                .create();

        DataSchema expectedSchema = SchemaBuilder.builderFrom(schema)
                .addNamed("Tax", double.class)
                .addNamed("TotalPrice", double.class)
                .build();

        assertThat(table.getSchema(), is(expectedSchema));

        DidoSubscriber didoSubscriber = SubscriberUtil.didoSubscriberFrom(table, table.getSchema());

        DidoData.withSchema(schema).many()
                .of(5, "Apple", 20.0)
                .of(8, "Pear", 30.0)
                .of(3, "Banana", 25.0)
                .toList().forEach(didoSubscriber::onData);

        LiveRow row = table.getRow(5);

        assertThat(row.getValueNamed("TotalPrice").getDouble(), is(22.0));
        table.close();
    }
}