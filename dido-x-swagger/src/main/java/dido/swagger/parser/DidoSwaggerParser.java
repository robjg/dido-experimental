package dido.swagger.parser;

import dido.data.DataSchema;
import io.swagger.parser.OpenAPIParser;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.media.MediaType;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.responses.ApiResponse;
import io.swagger.v3.parser.core.models.ParseOptions;
import io.swagger.v3.parser.core.models.SwaggerParseResult;

public class DidoSwaggerParser {

    private final SwaggerParseResult result;

    private final SchemaToDido schemaToDido;

    private DidoSwaggerParser(SwaggerParseResult result,
                              SchemaToDido schemaToDido) {
        this.result = result;
        this.schemaToDido = schemaToDido;
    }

    public static class Settings {

        private String resource;

        private ParseOptions parserOptions;

        public Settings resource(String resource) {
            this.resource = resource;
            return this;
        }

        public Settings parserOptions(ParseOptions parserOptions) {
            this.parserOptions = parserOptions;
            return this;
        }

        public DidoSwaggerParser create() {

            SwaggerParseResult result = new OpenAPIParser()
                    .readLocation(resource, null, parserOptions);

            return new DidoSwaggerParser(result, SchemaToDido.newInstance());
        }

    }

    public static Settings with() {
        return new Settings();
    }

    public DataSchema getSchemaForPath(String path) {

        PathItem pathItem = result.getOpenAPI().getComponents().getPathItems().get(path);

        Operation operation = pathItem.getGet();

        ApiResponse response = operation.getResponses().get("200");

        MediaType mediaType = response.getContent().get("application/json");

        Schema<?> schema = mediaType.getSchema();

        if (schema.getProperties() != null) {
            return getSchema(schema);
        }
        else if (schema.get$ref() != null) {
            String ref = schema.get$ref();

        }

        return null;
    }

    public DataSchema getSchema(String schemaName) {

        Schema<?> schema = result.getOpenAPI().getComponents().getSchemas().get(schemaName);

        return getSchema(schema);
    }

    protected DataSchema getSchema(Schema<?> schema) {

        return schemaToDido.toDidoSchema(schema);
    }
}
