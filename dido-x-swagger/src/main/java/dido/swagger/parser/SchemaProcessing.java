package dido.swagger.parser;

import io.swagger.v3.oas.models.media.*;

import java.util.Map;

/**
 * <a href="json-schema-generator/src/main/java/io/micronaut/jsonschema/generator/aggregator/TypeAggregator.java">TypeAggregator</a>
 * <a href="jsonschema2pojo-core/src/main/java/org/jsonschema2pojo/rules/TypeRule.java">TypeRule</a>
 */
public class SchemaProcessing {

    protected SchemaProcessing() {
    }

    public static SchemaProcessing newInstance() {
        return new SchemaProcessing();
    }

    @SuppressWarnings("rawtypes")
    public void onParent(Map<String, Schema> properties, SchemaWalker.Parent parentWalker) {

        for (Map.Entry<String, Schema> entry : properties.entrySet()) {
            Schema<?> fieldSchema = entry.getValue();
            String fieldName = entry.getKey();

            SchemaWalker.Value valueWalker = parentWalker.field(fieldName);
            schemaSwitch(fieldSchema, valueWalker);
        }

        parentWalker.complete();
    }

    public void schemaSwitch(Schema<?> schema, SchemaWalker.Value fieldWalker) {

        if (schema.getProperties() == null) {
            switch (schema) {
                case ArraySchema as -> onValueSchema(as, fieldWalker);
                case StringSchema ss -> onValueSchema(ss, fieldWalker);
                case BooleanSchema bs -> onValueSchema(bs, fieldWalker);
                case IntegerSchema is -> onValueSchema(is, fieldWalker);
                default -> defaultOnSchema(schema, fieldWalker);
            }
        }
        else {
            SchemaWalker.Parent parentWalker = fieldWalker.nested();
            onParent(schema.getProperties(), parentWalker);
        }
    }

    protected void onValueSchema(BooleanSchema schema, SchemaWalker.Value valueWalker) {

        valueWalker.addType(boolean.class);
    }

    protected void onValueSchema(IntegerSchema schema, SchemaWalker.Value valueWalker) {

        Class<?> type =
                switch (schema.getFormat()) {
                    case "int32" -> int.class;
                    case "int64" -> long.class;
                    default -> int.class;
                };

        valueWalker.addType(type);
    }

    protected void onValueSchema(StringSchema schema, SchemaWalker.Value valueWalker) {

        valueWalker.addType(String.class);
    }

    protected void onValueSchema(ArraySchema schema, SchemaWalker.Value valueWalker) {

        Schema<?> itemSchema = schema.getItems();

        schemaSwitch(itemSchema, valueWalker.array());
    }

    protected void defaultOnSchema(Schema<?> schema, SchemaWalker.Value valueWalker) {

        valueWalker.addType(Object.class);
    }

}
