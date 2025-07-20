package dido.operators.transform;

import dido.data.*;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class FieldReaderTest {

    static DataSchema schema = ArrayData.schemaBuilder()
            .addNamed("Fruit", String.class)
            .addNamed("Qty", int.class)
            .addNamed("Price", double.class)
            .build();

    DidoData data = ArrayData.valuesWithSchema(schema)
            .of("Apple", 10, 23.5);


    interface InteractionBuilder {

        FieldReader getterNamed(String name);

        FieldWriter writeNamed(String name, Type type);
    }

    interface Interaction {

        Runnable prepare(InteractionBuilder builder);
    }


    static class MarkupOperation implements Interaction {

        @Override
        public Runnable prepare(InteractionBuilder builder) {

            FieldReader priceGetter = builder.getterNamed("Price");

            FieldWriter markupSetter = builder.writeNamed("Markup", double.class);
            FieldWriter amountSetter = builder.writeNamed("MarkupAmount", double.class);
            FieldWriter finalSetter = builder.writeNamed("FinalPrice", double.class);


            return () -> {
                double price = priceGetter.getDouble();

                double markup;
                if (price > 100.0) {
                    markup = 0.3;
                } else {
                    markup = 0.5;
                }
                markupSetter.setDouble(markup);

                double markupAmount = price * markup;
                amountSetter.setDouble(markupAmount);

                finalSetter.setDouble(price + markupAmount);
            };
        }
    }

    static class FieldGetterReader implements FieldReader {

        final FieldGetter getter;

        DidoData current;

        FieldGetterReader(FieldGetter getter) {
            this.getter = getter;
        }

        @Override
        public Object get() {
            return getter.get(current);
        }

        @Override
        public double getDouble() {
            return getter.getDouble(current);
        }
    }

    static class FieldSetterWriter implements FieldWriter {

        private final Function<? super WriteSchema, ? extends FieldSetter> setterFunc;

        private FieldSetter setter;

        private WritableData writableData;

        FieldSetterWriter(Function<? super WriteSchema, ? extends FieldSetter> setterFunc) {
            this.setterFunc = setterFunc;
        }

        void init(WriteSchema schema) {
            this.setter = setterFunc.apply(schema);
        }

        @Override
        public void setDouble(double d) {
            setter.setDouble(writableData, d);
        }
    }

    static DidoTransform create() {

        ReadStrategy readStrategy = ReadStrategy.fromSchema(schema);

        DataFactoryProvider factoryProvider = DataFactoryProvider.newInstance();

        SchemaFactory schemaFactory = factoryProvider.getSchemaFactory();

        List<FieldGetterReader> getters = new ArrayList<>();
        List<FieldSetterWriter> setters = new ArrayList<>();

        Runnable process = new MarkupOperation().prepare(new InteractionBuilder() {
            @Override
            public FieldReader getterNamed(String name) {
                FieldGetter getter = readStrategy.getFieldGetterNamed(name);
                FieldGetterReader reader = new FieldGetterReader(getter);
                getters.add(reader);
                return reader;
            }

            @Override
            public FieldWriter writeNamed(String name, Type type) {
                schemaFactory.addSchemaField(SchemaField.of(0, name, type));
                FieldSetterWriter writer = new FieldSetterWriter(
                        writeSchema -> writeSchema.getFieldSetterNamed(name));
                setters.add(writer);
                return writer;
            }
        });

        WriteSchema outSchema = WriteSchema.from(schemaFactory.toSchema());
        setters.forEach(setter -> setter.init(outSchema));

        DataFactory factory = factoryProvider.factoryFor(outSchema);


        return new DidoTransform() {

            @Override
            public DataSchema getResultantSchema() {
                return outSchema;
            }

            @Override
            public DidoData apply(DidoData data) {

                getters.forEach(getter -> getter.current = data);
                WritableData writableData = factory.getWritableData();
                setters.forEach(setter -> setter.writableData = writableData);
                process.run();
                return factory.toData();
            }
        };
    }

    @Test
    void thinking() {

        DidoData result = create().apply(data);

        DataSchema expectedSchema = SchemaBuilder.newInstance()
                .addNamed("Markup", double.class)
                .addNamed("MarkupAmount", double.class)
                .addNamed("FinalPrice", double.class)
                .build();

        DidoData expectedData = ArrayData.valuesWithSchema(expectedSchema)
                .of(0.5, 11.75, 35.25);

        assertThat(result, is(expectedData));
    }
}