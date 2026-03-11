package dido.swagger.parser;

import io.swagger.parser.OpenAPIParser;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.media.*;
import io.swagger.v3.oas.models.responses.ApiResponse;
import io.swagger.v3.parser.core.models.ParseOptions;
import io.swagger.v3.parser.core.models.SwaggerParseResult;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class SwaggerAssumptionsTest {

    @Test
    void noOptions() {

        SwaggerParseResult result = new OpenAPIParser()
                .readLocation("petstore.yaml", null, null);

        assertThat(result.getMessages(), Matchers.empty());

        OpenAPI openAPI = result.getOpenAPI();

        Map<String, Schema> componentSchemas = openAPI.getComponents().getSchemas();

        assertThat(componentSchemas.keySet(), Matchers.contains("Pet", "Pets", "Error"));

        Schema petSchema = componentSchemas.get("Pet");

        Map<String, Schema> petSchemaProperties = petSchema.getProperties();

        assertThat(petSchemaProperties.get("id"), instanceOf(IntegerSchema.class));
        assertThat(petSchemaProperties.get("name"), instanceOf(StringSchema.class));
        assertThat(petSchemaProperties.get("tag"), instanceOf(StringSchema.class));

        Schema petsSchema = componentSchemas.get("Pets");

        assertThat(petsSchema, instanceOf(ArraySchema.class));
        assertThat(petsSchema.getItems().get$ref(),  is("#/components/schemas/Pet"));

    }

    @Test
    void resolveAndResolveFully() {

        ParseOptions parseOptions = new ParseOptions();
        parseOptions.setResolve(true);
        parseOptions.setResolveFully(true);

        SwaggerParseResult result = new OpenAPIParser()
                .readLocation("petstore.yaml", null, parseOptions);

        assertThat(result.getMessages(), Matchers.empty());

        OpenAPI openAPI = result.getOpenAPI();

        Map<String, PathItem> pathItems = openAPI.getPaths();

        assertThat(pathItems.keySet(), contains("/pets", "/pets/{petId}"));

        PathItem pathItem = pathItems.get("/pets");

        Operation operation = pathItem.getGet();

        ApiResponse response = operation.getResponses().get("200");

        MediaType mediaType = response.getContent().get("application/json");

        Schema<?> schemaOfPath = mediaType.getSchema();

        assertThat(schemaOfPath, Matchers.instanceOf(ArraySchema.class));

        Map<String, Schema> componentSchemas = openAPI.getComponents().getSchemas();

        assertThat(componentSchemas.keySet(), Matchers.contains("Pet", "Pets", "Error"));

        Schema petSchema = componentSchemas.get("Pet");

        Map<String, Schema> petSchemaProperties = petSchema.getProperties();

        assertThat(petSchemaProperties.get("id"), instanceOf(IntegerSchema.class));
        assertThat(petSchemaProperties.get("name"), instanceOf(StringSchema.class));
        assertThat(petSchemaProperties.get("tag"), instanceOf(StringSchema.class));

        Schema petsSchema = componentSchemas.get("Pets");

        assertThat(petsSchema, instanceOf(ArraySchema.class));
        assertThat(petsSchema.getItems(),  is(petSchema));

        assertThat(schemaOfPath, is(petsSchema));
    }

}
