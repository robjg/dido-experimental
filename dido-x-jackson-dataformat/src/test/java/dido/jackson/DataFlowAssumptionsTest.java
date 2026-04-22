package dido.jackson;

import dido.data.DidoData;
import dido.data.immutable.MapData;
import org.junit.jupiter.api.Test;
import tools.jackson.core.JsonGenerator;
import tools.jackson.core.ObjectWriteContext;
import tools.jackson.dataformat.yaml.YAMLFactory;

import java.io.StringWriter;
import java.lang.reflect.Type;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class DataFlowAssumptionsTest {

    @Test
    void apiUse() {

        YAMLFactory factory = new YAMLFactory();

        StringWriter writer = new StringWriter();

        DidoData data = MapData.builder()
                .withString("Fruit", "Apple")
                .withInt("Qty", 5)
                .withDouble("Price", 23.4)
                .build();

        JsonGenerator generator = factory.createGenerator(
                ObjectWriteContext.empty(), writer);

        generator.writeStartObject();
        data.getSchema().getSchemaFields().forEach(field -> {
            String fieldName = field.getName();
            Type fieldType = field.getType();
            if  (fieldType == int.class) {
                generator.writeNumberProperty(fieldName, data.getIntNamed(fieldName));
            }
            else if (fieldType == double.class) {
                generator.writeNumberProperty(fieldName, data.getDoubleNamed(fieldName));
            }
            else {
                generator.writeStringProperty(fieldName, data.getStringNamed(fieldName));
            }
        });
        generator.writeEndObject();

        // What's wrong here?
        String expected = """
                ---
                Fruit: "Apple"
                Qty: 5
                Price: 23.4""";

        assertThat(writer.toString(), is(expected));
    }
}
