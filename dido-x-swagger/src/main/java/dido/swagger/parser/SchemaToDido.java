package dido.swagger.parser;

import dido.data.DataSchema;
import dido.data.SchemaFactory;
import dido.data.SchemaField;
import io.swagger.v3.oas.models.media.Schema;

import java.lang.reflect.Type;
import java.util.function.Supplier;

public class SchemaToDido {

    private final Supplier<SchemaFactory> schemaFactorySupplier;

    private final SchemaProcessing schemaProcessing;

    public SchemaToDido(Supplier<SchemaFactory> schemaFactory,
                        SchemaProcessing typeExtractor) {
        this.schemaFactorySupplier = schemaFactory;
        this.schemaProcessing = typeExtractor;
    }

    public static class Settings {

        private Supplier<SchemaFactory> schemaFactorySupplier;

        private SchemaProcessing typeExtractor;

        public Settings schemaFactory(Supplier<SchemaFactory> schemaFactorySupplier) {
            this.schemaFactorySupplier = schemaFactorySupplier;
            return this;
        }

        public Settings schemaProcessing(SchemaProcessing typeExtractor) {
            this.typeExtractor = typeExtractor;
            return this;
        }

        public SchemaToDido create() {
            return new SchemaToDido(
                    schemaFactorySupplier == null ?
                            SchemaFactory::newInstance : schemaFactorySupplier,
                    typeExtractor == null ?
                            SchemaProcessing.newInstance() : typeExtractor);
        }
    }

    public static Settings with() {
        return new Settings();
    }

    public static SchemaToDido newInstance() {
        return with().create();
    }

    public DataSchema toDidoSchema(Schema<?> jsonSchema) {

        RootWalker rootWalker = new RootWalker();

        schemaProcessing.schemaSwitch(jsonSchema, rootWalker);

        return rootWalker.schema;
    }

    class RootWalker implements SchemaWalker.Value {

        private String reference;

        private DataSchema schema;

        @Override
        public void addType(Type type) {
            throw new UnsupportedOperationException("Root Schemas can't be Value Schemas");
        }

        @Override
        public void addReference(String reference) {

            this.reference = reference;
        }

        @Override
        public Parent nested() {
            return new ParentWalker() {
                @Override
                public void complete() {
                    schema = schemaFactory.toSchema();
                }
            };
        }

        @Override
        public Value array() {
            return this;
        }
    }


    abstract class ParentWalker implements SchemaWalker.Parent {

        protected final SchemaFactory schemaFactory;

        ParentWalker() {
            this.schemaFactory = schemaFactorySupplier.get();
        }

        @Override
        public Value field(String fieldName) {
            return new FieldWalker(this, fieldName);
        }
    }


    class FieldWalker implements SchemaWalker.Value {

        private final ParentWalker parentWalker;

        private final String fieldName;

        FieldWalker(ParentWalker parentWalker, String fieldName) {
            this.parentWalker = parentWalker;
            this.fieldName = fieldName;
        }

        @Override
        public Parent nested() {
            return null;
        }

        @Override
        public Value array() {
            return null;
        }

        @Override
        public void addType(Type type) {

            parentWalker.schemaFactory.addSchemaField(SchemaField.of(0, fieldName, type));
        }

        @Override
        public void addReference(String reference) {

        }
    }

    class NestedParentWalker extends ParentWalker {

        private final FieldWalker fieldWalker;

        NestedParentWalker(FieldWalker fieldWalker) {
            this.fieldWalker = fieldWalker;
        }

        @Override
        public void complete() {
            fieldWalker.parentWalker.schemaFactory.addSchemaField(
                    SchemaField.ofNested(0, fieldWalker.fieldName, schemaFactory.toSchema()));
        }
    }
}
