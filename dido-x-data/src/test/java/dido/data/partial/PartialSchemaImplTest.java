package dido.data.partial;

import dido.data.DataSchema;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class PartialSchemaImplTest {

    @Test
    void partialOfFields() {

        DataSchema schema = DataSchema.builder()
                .addNamed("Fruit", String.class)
                .addNamed("Qty", int.class)
                .addNamed("Price", double.class)
                .build();
        PartialSchema partialSchema = PartialSchemaImpl.of(schema, "Fruit", "Price");

        DataSchema expected = DataSchema.builder()
                .addNamedAt(1, "Fruit", String.class)
                .addNamedAt(3, "Price", double.class)
                .build();

        assertThat(partialSchema, is(expected));
    }

    @Test
    void outOfOrderFields() {

        DataSchema schema = DataSchema.builder()
                .addNamed("Fruit", String.class)
                .addNamed("Qty", int.class)
                .addNamed("Price", double.class)
                .build();
        PartialSchema partialSchema = PartialSchemaImpl.of(schema, "Price", "Fruit");

        DataSchema expected = DataSchema.builder()
                .addNamedAt(1, "Fruit", String.class)
                .addNamedAt(3, "Price", double.class)
                .build();

        assertThat(partialSchema, is(expected));
    }

    @Test
    void ofSet() {

        DataSchema schema = DataSchema.builder()
                .addNamed("Fruit", String.class)
                .addNamed("Qty", int.class)
                .addNamed("Price", double.class)
                .build();
        PartialSchema partialSchema = PartialSchemaImpl.of(schema,
                "Price", "Fruit");

        DataSchema expected = DataSchema.builder()
                .addNamedAt(1, "Fruit", String.class)
                .addNamedAt(3, "Price", double.class)
                .build();

        assertThat(partialSchema, is(expected));
    }

    @Test
    void partialOfSame() {

        DataSchema schema = DataSchema.builder()
                .addNamed("Fruit", String.class)
                .addNamed("Qty", int.class)
                .addNamed("Price", double.class)
                .build();

        PartialSchema partialSchema = PartialSchemaImpl.of(schema, "Fruit", "Price", "Qty");

        assertThat(partialSchema, is(schema));
    }
}