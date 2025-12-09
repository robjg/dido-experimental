package dido.operators.transform;

import dido.data.*;

import java.lang.reflect.Type;
import java.util.*;
import java.util.function.Function;

public class OperationTransformBuilder {

    private final ReadSchema readSchema;

    private final DataFactoryProvider dataFactoryProvider;

    private final List<OperationDefinition> opDefs = new ArrayList<>();

    private final boolean existingFields;

    private final boolean reIndex;

    private OperationTransformBuilder(ReadSchema readSchema,
                                      Settings settings) {
        this.readSchema = readSchema;
        this.dataFactoryProvider = Objects.requireNonNullElse(
                settings.dataFactoryProvider, DataFactoryProvider.newInstance());
        this.existingFields = settings.existingFields;
        this.reIndex = settings.reIndex;
    }

    public static class Settings {

        private DataFactoryProvider dataFactoryProvider;

        private boolean existingFields;

        private boolean reIndex;

        public Settings dataFactoryProvider(DataFactoryProvider dataFactoryProvider) {
            this.dataFactoryProvider = dataFactoryProvider;
            return this;
        }

        public Settings existingFields(boolean existingFields) {
            this.existingFields = existingFields;
            return this;
        }

        public Settings reIndex(boolean reIndex) {
            this.reIndex = reIndex;
            return this;
        }

        public OperationTransformBuilder forSchema(DataSchema incomingSchema) {

            return new OperationTransformBuilder(ReadSchema.from(incomingSchema),
                    this);
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


    public DidoTransform build() {

        SchemaFactory schemaFactory = dataFactoryProvider.getSchemaFactory();

        Getters getters = new Getters();
        Setters setters = new Setters();

        Map<String, Runnable> copyProcs = new LinkedHashMap<>(); ;

        OperationContext context = new OperationContext() {

            @Override
            public Type typeOfNamed(String name) {
                return readSchema.getTypeNamed(name);
            }

            @Override
            public ValueGetter getterNamed(String name) {
                Objects.requireNonNull(name);
                return getters.newGetter(readSchema.getFieldGetterNamed(name),
                        readSchema.getTypeNamed(name));
            }

            @Override
            public ValueSetter setterNamed(String name, Type type) {
                Objects.requireNonNull(name, "No name");
                Objects.requireNonNull(type, "No type");
                SchemaField existing = schemaFactory.getSchemaFieldNamed(name);
                if (existing != null) {
                    if (type != existing.getType()) {
                        schemaFactory.addSchemaField(SchemaField.of(
                                existing.getIndex(), existing.getName(), type));
                    }
                }
                else {
                    schemaFactory.addSchemaField(SchemaField.of(0, name, type));
                }
                copyProcs.remove(name);
                return setters.newSetter(
                        name,
                        writeSchema -> writeSchema.getFieldSetterNamed(name),
                        type);
            }

            @Override
            public void removeNamed(String name) {
                copyProcs.remove(name);
                schemaFactory.removeNamed(name);
                setters.removeSetter(name);
            }
        };

        if (existingFields) {
            for (SchemaField schemaField: readSchema.getSchemaFields()) {
                schemaFactory.addSchemaField(schemaField);
                Runnable copy = BasicOperations.copyNamed(schemaField.getName())
                        .prepare(context);
                copyProcs.put(schemaField.getName(), copy);
            }
        }

        List<Runnable> opProcesses = opDefs.stream()
                .map(opDef -> opDef.prepare(context))
                .toList();

        List<Runnable> processes = new ArrayList<>(copyProcs.values());
        processes.addAll(opProcesses);

        DataSchema newSchema;
        if (reIndex) {
            SchemaFactory factory2 = dataFactoryProvider.getSchemaFactory();
            for (SchemaField schemaField : schemaFactory.getSchemaFields()) {
                factory2.addSchemaField(schemaField.mapToIndex(0));
            }
            newSchema = factory2.toSchema();
        }
        else {
            newSchema = schemaFactory.toSchema();
        }

        DataFactory factory = dataFactoryProvider.factoryFor(newSchema);
        setters.init(factory.getSchema());

        return new DidoTransform() {

            @Override
            public DataSchema getResultantSchema() {
                return factory.getSchema();
            }

            @Override
            public DidoData apply(DidoData data) {

                getters.current = data;
                setters.writableData = factory.getWritableData();
                processes.forEach(Runnable::run);
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

        Map<String, FieldValueSetter> setters = new LinkedHashMap<>();

        private WritableData writableData;

        class FieldValueSetter  implements ValueSetter {

            private final Function<? super WriteSchema, ? extends FieldSetter> setterFunc;

            protected FieldSetter setter;

            final Type type;

            FieldValueSetter(Function<? super WriteSchema, ? extends FieldSetter> setterFunc,
                             Type type) {
                this.setterFunc = setterFunc;
                this.type = type;
            }

            void init(WriteSchema schema) {
                this.setter = setterFunc.apply(schema);
            }

            @Override
            public Type getType() {
                return type;
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


        ValueSetter newSetter(String name, Function<? super WriteSchema, ? extends FieldSetter> setterFunc,
                              Type type) {
            FieldValueSetter writer = new FieldValueSetter(setterFunc, type);
            setters.put(name, writer);
            return writer;
        }

        void removeSetter(String name) {
            setters.remove(name);
        }

        void init(WriteSchema outSchema) {
            setters.values().forEach(setter -> setter.init(outSchema));
        }
    }
}
