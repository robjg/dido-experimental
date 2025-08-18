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

        SchemaFactory schemaFactory = dataFactoryProvider.getSchemaFactory();

        Getters getters = new Getters();
        Setters setters = new Setters();

        OperationContext context = new OperationContext() {
            @Override
            public <T> ValueGetter<T> valueGetterNamed(String name) {
                return getters.newGetter(readSchema.getFieldGetterNamed(name),
                        readSchema.getTypeNamed(name));
            }

            @Override
            public DoubleGetter doubleGetterNamed(String name) {
                return getters.newDoubleGetter(readSchema.getFieldGetterNamed(name));
            }

            @Override
            public <T> ValueSetter<T> valueSetterNamed(String name, Type type) {
                schemaFactory.addSchemaField(SchemaField.of(0, name, type));

                return setters.newSetter(
                        writeSchema -> writeSchema.getFieldSetterNamed(name));
            }

            @Override
            public DoubleSetter doubleSetterNamed(String name) {
                schemaFactory.addSchemaField(SchemaField.of(0, name, double.class));

                return setters.newDoubleSetter(
                        writeSchema -> writeSchema.getFieldSetterNamed(name));
            }
        };

        List<Runnable> process = opDefs.stream()
                .map(opDef -> opDef.prepare(context))
                .toList();

        WriteSchema outSchema = WriteSchema.from(schemaFactory.toSchema());
        setters.init(outSchema);

        DataFactory factory = dataFactoryProvider.factoryFor(outSchema);

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

        class FieldValueGetter<T> implements ValueGetter<T> {

            final FieldGetter getter;

            final Type type;

            FieldValueGetter(FieldGetter getter, Type type) {
                this.getter = getter;
                this.type = type;
            }

            @Override
            public Type getType() {
                return type;
            }

            @Override
            public boolean has() {
                return getter.has(current);
            }

            @Override
            public T get() {
                //noinspection unchecked
                return (T) getter.get(current);
            }
        }

        class FieldDoubleGetter implements DoubleGetter {

            final FieldGetter getter;

            FieldDoubleGetter(FieldGetter getter) {
                this.getter = getter;
            }

            @Override
            public boolean has() {
                return getter.has(current);
            }

            @Override
            public double getDouble() {
                return getter.getDouble(current);
            }
        }

        public <T> ValueGetter<T> newGetter(FieldGetter getter, Type type) {
            return new FieldValueGetter<>(getter, type);
        }

        public DoubleGetter newDoubleGetter(FieldGetter getter) {
            return new FieldDoubleGetter(getter);
        }
    }

    static class Setters {

        List<BaseSetter<?>> setters = new ArrayList<>();

        private WritableData writableData;

        abstract class BaseSetter<T> implements ValueSetter<T> {

            private final Function<? super WriteSchema, ? extends FieldSetter> setterFunc;

            protected FieldSetter setter;

            BaseSetter(Function<? super WriteSchema, ? extends FieldSetter> setterFunc) {
                this.setterFunc = setterFunc;
            }

            void init(WriteSchema schema) {
                this.setter = setterFunc.apply(schema);
            }

            @Override
            public void clear() {
                setter.clear(writableData);
            }
        }

        class FieldValueSetter<T> extends BaseSetter<T> {

            FieldValueSetter(Function<? super WriteSchema, ? extends FieldSetter> setterFunc) {
                super(setterFunc);
            }

            @Override
            public void set(T value) {
                setter.set(writableData, value);
            }
        }

        class FieldDoubleSetter extends BaseSetter<Double> implements DoubleSetter {

            FieldDoubleSetter(Function<? super WriteSchema, ? extends FieldSetter> setterFunc) {
                super(setterFunc);
            }

            @Override
            public void setDouble(double d) {
                setter.setDouble(writableData, d);
            }
        }

        <T> ValueSetter<T> newSetter(Function<? super WriteSchema, ? extends FieldSetter> setterFunc) {
            FieldValueSetter<T> writer = new FieldValueSetter<>(setterFunc);
            setters.add(writer);
            return writer;
        }

        DoubleSetter newDoubleSetter(Function<? super WriteSchema, ? extends FieldSetter> setterFunc) {
            FieldDoubleSetter writer = new FieldDoubleSetter(setterFunc);
            setters.add(writer);
            return writer;
        }

        void init(WriteSchema outSchema) {
            setters.forEach(setter -> setter.init(outSchema));
        }
    }
}
