package dido.table.internal;

import dido.data.DataSchema;
import dido.data.DidoData;
import dido.data.SchemaFactory;
import dido.operators.transform.BasicOperations;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;

class LiveOperationBuilderTest {


    @Test
    void idea() {

        DataSchema schema = DataSchema.builder()
                .addNamed("Qty", int.class)
                .build();

        SchemaFactory schemaFactory = SchemaFactory.newInstanceFrom(schema);

        LiveOperationBuilder test = LiveOperationBuilder.forSchema(schema);

        LiveOperation consumer = test.addOp(BasicOperations.map()
                        .from("Qty")
                        .with().intOp(qty -> qty * 2))
                .build();

        ArrayRowImpl row = new ArrayRowImpl(schemaFactory.toSchema(), null);
        row.onData(DidoData.withSchema(schema).of(2), consumer);

        assertThat(row.getValueNamed("Qty").get(), Matchers.is(4));
    }

}