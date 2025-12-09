package dido.operators.transform;

import dido.data.DataSchema;
import dido.data.DidoData;
import dido.data.immutable.ArrayData;
import dido.data.schema.SchemaBuilder;
import org.junit.jupiter.api.Test;

import java.util.function.Function;
import java.util.function.UnaryOperator;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class BasicOperationsTest {

    static DataSchema schema = ArrayData.schemaBuilder()
            .addNamed("Fruit", String.class)
            .addNamed("Qty", int.class)
            .addNamed("Price", double.class)
            .build();

    DidoData data = ArrayData.withSchema(schema)
            .of("Apple", 10, 23.5);

    @Test
    void copyWithFieldBuilder() {

        DidoTransform transformation = OperationTransformBuilder.with()
                .reIndex(true)
                .forSchema(schema)
                .addOp(BasicOperations.copy().from("Price").with().op()) // copies Price
                .addOp(BasicOperations.copy().from("Qty").with().op()) // copies Qty to index 2
                .build();

        DidoData result = transformation.apply(data);

        DataSchema expectedSchema = DataSchema.builder()
                .addNamed("Price", double.class)
                .addNamed("Qty", int.class)
                .build();

        assertThat(transformation.getResultantSchema(), is(expectedSchema));

        DidoData expectedData = ArrayData.withSchema(expectedSchema)
                .of(23.5, 10);

        assertThat(result, is(expectedData));
    }

    @Test
    void copyWithConversion() {

        DidoTransform transformation = OperationTransformBuilder.with()
                .existingFields(true)
                .reIndex(true)
                .forSchema(schema)
                .addOp(BasicOperations.copy()
                        .from("Qty")
                        .with().type(String.class)
                        .op()) // copies Qty
                .addOp(BasicOperations.copy()
                        .from("Price")
                        .with().type(String.class).op()) // copies Price
                .build();

        DidoData result = transformation.apply(data);

        DataSchema expectedSchema = DataSchema.builder()
                .addNamed("Fruit", String.class)
                .addNamed("Qty", String.class)
                .addNamed("Price", String.class)
                .build();

        assertThat(transformation.getResultantSchema(), is(expectedSchema));

        DidoData expectedData = ArrayData.withSchema(expectedSchema)
                .of("Apple", "10", "23.5");

        assertThat(result, is(expectedData));
    }

    @Test
    void copyNamed() {

        DidoTransform transformation = OperationTransformBuilder.with()
                .reIndex(true)
                .forSchema(schema)
                .addOp(BasicOperations.copyNamed("Price"))
                .addOp(BasicOperations.copyNamed("Fruit"))
                .build();

        DidoData result = transformation.apply(data);

        DataSchema expectedSchema = DataSchema.builder()
                .addNamed("Price", double.class)
                .addNamed("Fruit", String.class)
                .build();

        assertThat(transformation.getResultantSchema(), is(expectedSchema));

        DidoData expectedData = ArrayData.withSchema(expectedSchema)
                .of(23.5, "Apple");

        assertThat(result, is(expectedData));
    }

    @Test
    void copyNamedFromTo() {

        DidoTransform transformation = OperationTransformBuilder
                .forSchema(schema)
                .addOp(BasicOperations.copyNamed("Fruit", "Type"))
                .addOp(BasicOperations.copyNamed("Price", "Price"))
                .build();

        DidoData result = transformation.apply(data);

        DataSchema expectedSchema = DataSchema.builder()
                .addNamed("Type", String.class)
                .addNamed("Price", double.class)
                .build();

        assertThat(transformation.getResultantSchema(), is(expectedSchema));

        DidoData expectedData = ArrayData.withSchema(expectedSchema)
                .of("Apple", 23.5);

        assertThat(result, is(expectedData));
    }

    @Test
    void copySameName() {

        DidoTransform transformation = OperationTransformBuilder
                .forSchema(schema)
                .addOp(BasicOperations.copyNamed("Fruit", "Fruit"))
                .addOp(BasicOperations.copyNamed("Qty", "Qty"))
                .addOp(BasicOperations.copyNamed("Price", "Price"))
                .build();

        DidoData result = transformation.apply(data);

        assertThat(transformation.getResultantSchema(), is(schema));

        assertThat(result, is(data));
    }

    @Test
    void rename() {

        DidoTransform transformation = OperationTransformBuilder
                .with()
                .existingFields(true)
                .forSchema(schema)
                .addOp(BasicOperations.rename("Qty", "Quantity"))
                .addOp(BasicOperations.rename("Price", "ThePrice"))
                .addOp(BasicOperations.rename("Fruit", "Type"))
                .build();

        DataSchema expectedSchema = DataSchema.builder()
                .addNamedAt(4, "Quantity", int.class)
                .addNamedAt(5, "ThePrice", double.class)
                .addNamedAt(6, "Type", String.class)
                .build();

        assertThat(transformation.getResultantSchema(), is(expectedSchema));

        DidoData result = transformation.apply(data);

        DidoData expectedData = ArrayData.withSchema(expectedSchema)
                .of(10, 23.5, "Apple");

        assertThat(result, is(expectedData));
    }

    @Test
    void setNamedWithCopy() {

        DidoTransform transformation = OperationTransformBuilder.with()
                .existingFields(true)
                .forSchema(schema)
                .addOp(BasicOperations.setNamed("Fruit", "Orange"))
                .addOp(BasicOperations.setNamed("Qty", 1234L, long.class))
                .addOp(BasicOperations.setNamed("InStock", true, boolean.class))
                .build();

        DidoData result = transformation.apply(data);

        DataSchema expectedSchema = DataSchema.builder()
                .addNamed("Fruit", String.class)
                .addNamed("Qty", long.class)
                .addNamed("Price", double.class)
                .addNamed("InStock", boolean.class)
                .build();

        assertThat(transformation.getResultantSchema(), is(expectedSchema));

        DidoData expectedData = ArrayData.withSchema(expectedSchema)
                .of("Orange", 1234L, 23.5, true);

        assertThat(result, is(expectedData));
    }

    @Test
    void setWithFieldLocBuilder() {

        DidoTransform transformation = OperationTransformBuilder.with()
                .forSchema(schema)
                .addOp(BasicOperations.set().named("Price").with()
                        .value(54.3).op()) // copies Price to index 3
                .addOp(BasicOperations.set().named("Qty").with()
                        .value(5).op()) // copies Qty to index 2
                .addOp(BasicOperations.set().named("Fruit").with()
                        .value("Orange").op()) // copies Fruit to index 1
                .addOp(BasicOperations.set().named("InStock").with()
                        .value(true).type(boolean.class).op()) // copies Qty to index 2
                .build();

        DidoData result = transformation.apply(data);

        DataSchema expectedSchema = DataSchema.builder()
                .addNamed("Price", double.class)
                .addNamed("Qty", int.class)
                .addNamed("Fruit", String.class)
                .addNamed("InStock", boolean.class)
                .build();

        assertThat(transformation.getResultantSchema(), is(expectedSchema));

        DidoData expectedData = DidoData.of(54.3, 5, "Orange", true);

        assertThat(result, is(expectedData));
    }

    @Test
    void removeNamed() {

        DidoTransform transformation = OperationTransformBuilder.with()
                .existingFields(true)
                .reIndex(true)
                .forSchema(schema)
                .addOp(BasicOperations.removeNamed("Fruit"))
                .addOp(BasicOperations.removeNamed("Price"))
                .build();

        DidoData result = transformation.apply(data);

        DataSchema expectedSchema = DataSchema.builder()
                .addNamed("Qty", int.class)
                .build();

        assertThat(transformation.getResultantSchema(), is(expectedSchema));

        DidoData expectedData = ArrayData.withSchema(expectedSchema)
                .of(10);

        assertThat(result, is(expectedData));
    }

    @Test
    void mapNew() {

        UnaryOperator<String> fruitOp = String::toUpperCase;
        Function<Integer, Double> qtyOp = qty -> (double) qty * 2.5;
        Function<Double, String> priceOp = price -> "£" + price;

        DidoTransform transformation = OperationTransformBuilder.with()
                .forSchema(schema)
                .addOp(BasicOperations.map()
                        .from("Fruit")
                        .with().func(fruitOp))
                .addOp(BasicOperations.map()
                        .from("Qty")
                        .with().type(double.class)
                        .func(qtyOp))
                .addOp(BasicOperations.map()
                        .from("Price")
                        .to("DisplayPrice")
                        .with().type(String.class)
                        .func(priceOp))
                .build();

        DidoData result = transformation.apply(data);

        DataSchema expectedSchema = SchemaBuilder.newInstance()
                .addNamed("Fruit", String.class)
                .addNamed("Qty", double.class)
                .addNamed("DisplayPrice", String.class)
                .build();

        assertThat(transformation.getResultantSchema(), is(expectedSchema));

        DidoData expectedData = ArrayData.withSchema(schema)
                .of("APPLE", 25.0, "£23.5");

        assertThat(result, is(expectedData));
    }

    @Test
    void mapExisting() {

        DidoTransform transformation = OperationTransformBuilder.with()
                .existingFields(true)
                .forSchema(schema)
                .addOp(BasicOperations.map()
                        .from("Qty")
                                .with().func(qty -> (int) qty * 2))
                .build();

        DidoData result = transformation.apply(data);

        assertThat(transformation.getResultantSchema(), is(schema));

        DidoData expectedData = ArrayData.withSchema(schema)
                .of("Apple", 20, 23.5);

        assertThat(result, is(expectedData));
    }

    @Test
    void unaryMapNewField() {

        DidoTransform transformation = OperationTransformBuilder.with()
                .existingFields(true)
                .forSchema(schema)
                .addOp(BasicOperations.map()
                        .from("Qty")
                        .to("Extra")
                        .with()
                        .func(qty -> (int) qty * 2))
                .build();

        DidoData result = transformation.apply(data);

        DataSchema expectedSchema = SchemaBuilder.newInstance()
                .merge(schema)
                .addNamed("Extra", int.class)
                .build();

        assertThat(transformation.getResultantSchema(), is(expectedSchema));

        DidoData expectedData = ArrayData.withSchema(expectedSchema)
                .of("Apple", 10, 23.5, 20);

        assertThat(result, is(expectedData));
    }

    @Test
    void mapIntToIntNamed() {

        DidoTransform transformation = OperationTransformBuilder.with()
                .existingFields(true)
                .forSchema(schema)
                .addOp(BasicOperations.map()
                        .from("Qty")
                        .with().intOp(qty -> qty * 2))
                .build();

        DidoData result = transformation.apply(data);

        assertThat(transformation.getResultantSchema(), is(schema));

        DidoData expectedData = ArrayData.withSchema(schema)
                .of("Apple", 20, 23.5);

        assertThat(result, is(expectedData));
    }

    @Test
    void mapLongToLongNamedAt() {

        DataSchema schema = ArrayData.schemaBuilder()
                .addNamedAt(15, "BigNumber", long.class)
                .build();

        DidoData data = DidoData.withSchema(schema)
                .of(1000L);

        DidoTransform transformation = OperationTransformBuilder.with()
                .existingFields(true)
                .forSchema(schema)
                .addOp(BasicOperations.map()
                        .from("BigNumber")
                        .with().longOp(qty -> qty * 2))
                .addOp(BasicOperations.map()
                        .from("BigNumber")
                        .to("AnotherBigNumber")
                        .with().longOp(qty -> qty * 2))
                .build();

        DidoData result = transformation.apply(data);

        DataSchema expectedSchema = ArrayData.schemaBuilder()
                .addNamedAt(15, "BigNumber", long.class)
                .addNamedAt(16,"AnotherBigNumber", long.class)
                .build();

        assertThat(transformation.getResultantSchema(), is(expectedSchema));

        DidoData expectedData = ArrayData.withSchema(expectedSchema)
                .of(2000L, 2000L);

        assertThat(result, is(expectedData));
    }

    @Test
    void mapDoubleToDouble() {

        DidoTransform transformation = OperationTransformBuilder.with()
                .existingFields(true)
                .forSchema(schema)
                .addOp(BasicOperations.map()
                        .from("Price")
                        .with().doubleOp(price -> price * 2))
                .build();

        DidoData result = transformation.apply(data);

        assertThat(transformation.getResultantSchema(), is(schema));

        DidoData expectedData = ArrayData.withSchema(schema)
                .of("Apple", 10, 47.0);

        assertThat(result, is(expectedData));
    }

}