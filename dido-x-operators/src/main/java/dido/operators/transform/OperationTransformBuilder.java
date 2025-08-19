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
            public ValueGetter getterNamed(String name) {
                return getters.newGetter(readSchema.getFieldGetterNamed(name),
                        readSchema.getTypeNamed(name));
            }

            @Override
            public ValueSetter setterNamed(String name, Type type) {
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

        class FieldValueGetter implements ValueGetter {

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
            public Object get() {
                return getter.get(current);
            }

            @Override
            public boolean getBoolean() {
                return getter.getBoolean(current);
            }

            @Override
            public char getChar() {
                return getter.getChar(current);
            }

            @Override
            public byte getByte() {
                return getter.getByte(current);
            }

            @Override
            public short getShort() {
                return getter.getShort(current);
            }

            @Override
            public int getInt() {
                return getter.getInt(current);
            }

            @Override
            public long getLong() {
                return getter.getLong(current);
            }

            @Override
            public float getFloat() {
                return getter.getFloat(current);
            }

            @Override
            public double getDouble() {
                return getter.getDouble(current);
            }

            @Override
            public String getString() {
                return getter.getString(current);
            }

        }


        public ValueGetter newGetter(FieldGetter getter, Type type) {
            return new FieldValueGetter(getter, type);
        }
    }

    static class Setters {

        List<FieldValueSetter> setters = new ArrayList<>();

        private WritableData writableData;

        class FieldValueSetter  implements ValueSetter {

            private final Function<? super WriteSchema, ? extends FieldSetter> setterFunc;

            protected FieldSetter setter;

            FieldValueSetter(Function<? super WriteSchema, ? extends FieldSetter> setterFunc) {
                this.setterFunc = setterFunc;
            }

            void init(WriteSchema schema) {
                this.setter = setterFunc.apply(schema);
            }

            @Override
            public void clear() {
                setter.clear(writableData);
            }

            @Override
            public void set(Object value) {
                setter.set(writableData, value);
            }

            @Override
            public void setBoolean(boolean value) {
                setter.setBoolean(writableData, value);
            }

            @Override
            public void setByte(byte value) {
                setter.setByte(writableData, value);
            }

            @Override
            public void setChar(char value) {
                setter.setChar(writableData, value);
            }

            @Override
            public void setShort(short value) {
                setter.setShort(writableData, value);
            }

            @Override
            public void setInt(int value) {
                setter.setInt(writableData, value);
            }

            @Override
            public void setLong(long value) {
                setter.setLong(writableData, value);
            }

            @Override
            public void setFloat(float value) {
                setter.setFloat(writableData, value);
            }

            @Override
            public void setDouble(double d) {
                setter.setDouble(writableData, d);
            }

            @Override
            public void setString(String value) {
                setter.setString(writableData, value);
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
