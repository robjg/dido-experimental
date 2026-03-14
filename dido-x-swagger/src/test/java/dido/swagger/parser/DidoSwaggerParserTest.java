package dido.swagger.parser;

import dido.data.DataSchema;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class DidoSwaggerParserTest {

    @Test
    void schemaComponent() {

        DidoSwaggerParser test = DidoSwaggerParser.with()
                .resource("petstore.yaml")
                .create();

        DataSchema schema = test.getSchema("Pet");

        DataSchema expectedSchema = DataSchema.builder()
                .addNamed("id", long.class)
                .addNamed("name", String.class)
                .addNamed("tag", String.class)
                .build();

        assertThat(schema, is(expectedSchema));
    }
}