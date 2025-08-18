package dido.operators.transform;

import dido.data.*;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

public class OperationTransformBuilder {

    private final ReadSchema readSchema;

    private final DataFactoryProvider dataFactoryProvider;

    private final List<OperationDefinition> opDefs = new ArrayList<>();

    private OperationTransformBuilder(ReadSchema readSchema, DataFactoryProvider dataFactoryProvider) {
        this.readSchema = readSchema;
        this.dataFactoryProvider = dataFactoryProvider;
    }

    public static class Settings {

        private DataFactoryProvider dataFactoryProvider;

        public Settings dataFactoryProvider(DataFactoryProvider dataFactoryProvider) {
            this.dataFactoryProvider = dataFactoryProvider;
            return this;
        }

        public OperationTransformBuilder forSchema(DataSchema incomingSchema) {

            DataFactoryProvider dataFactoryProvider = Objects.requireNonNullElse(
                    this.dataFactoryProvider, DataFactoryProvider.newInstance());

            return new OperationTransformBuilder(ReadSchema.from(incomingSchema),
                    dataFactoryProvider);
        }
    }

    public static Settings with() {
        return new Settings();
    }

    public static OperationTransformBuilder forSchema(DataSchema incomingSchema) {
        return with().forSchema(incomingSchema);
    }

    public OperationTransformBuilder addOp(OperationDefinition opDef) {
        opDefs.add(opDef);
        return this;
    }


    public DidoTransform create() {

        DataFactoryProvider factoryProvider = DataFactoryProvider.newInstance();

        SchemaFactory schemaFactory = factoryProvider.getSchemaFactory();

        Getters getters = new Getters();
        Setters setters = new Setters();

        OperationContext context = new OperationContext() {
            @Override
            public ValueGetter getterNamed(String name) {
                FieldGetter getter = readSchema.getFieldGetterNamed(name);
                return getters.newGetter(getter);
            }

            @Override
            public ValueSetter writeNamed(String name, Type type) {
                schemaFactory.addSchemaField(SchemaField.of(0, name, type));

                return setters.newSetter(
                        writeSchema -> writeSchema.getFieldSetterNamed(name));
            }
        };

        List<Runnable> process = opDefs.stream()
                .map(opDef -> opDef.prepare(context))
                .toList();

        WriteSchema outSchema = WriteSchema.from(schemaFactory.toSchema());
        setters.init(outSchema);

        DataFactory factory = factoryProvider.factoryFor(outSchema);

        return new DidoTransform() {

            @Override
            public DataSchema getResultantSchema() {
                return outSchema;
            }

            @Override
            public DidoData apply(DidoData data) {

                getters.current = data;
                setters.writableData = factory.getWritableData();
                process.forEach(Runnable::run);
                return factory.toData();
            }
        };
    }

    static class Getters {

        DidoData current;

        class FieldValueGetter extends AbstractValueGetter {

            final FieldGetter getter;

            FieldValueGetter(FieldGetter getter) {
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

        public ValueGetter newGetter(FieldGetter getter) {
            return new FieldValueGetter(getter);
        }
    }

    static class Setters {

        List<FieldValueSetter> setters = new ArrayList<>();

        private WritableData writableData;

        class FieldValueSetter extends AbstractValueSetter {

            private final Function<? super WriteSchema, ? extends FieldSetter> setterFunc;

            private FieldSetter setter;

            FieldValueSetter(Function<? super WriteSchema, ? extends FieldSetter> setterFunc) {
                this.setterFunc = setterFunc;
            }

            void init(WriteSchema schema) {
                this.setter = setterFunc.apply(schema);
            }

            @Override
            public void set(Object value) {
                setter.set(writableData, value);
            }

            @Override
            public void setDouble(double d) {
                setter.setDouble(writableData, d);
            }
        }

        ValueSetter newSetter(Function<? super WriteSchema, ? extends FieldSetter> setterFunc) {
            FieldValueSetter writer = new FieldValueSetter(setterFunc);
            setters.add(writer);
            return writer;
        }

        void init(WriteSchema outSchema) {
            setters.forEach(setter -> setter.init(outSchema));
        }
    }
}
