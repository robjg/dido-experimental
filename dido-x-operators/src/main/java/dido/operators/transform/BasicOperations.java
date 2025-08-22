package dido.operators.transform;

import dido.data.NoSuchFieldException;
import dido.data.SchemaField;
import dido.how.conversion.DefaultConversionProvider;
import dido.how.conversion.DidoConversionProvider;

import java.lang.reflect.Type;
import java.util.Objects;
import java.util.function.DoubleUnaryOperator;
import java.util.function.Function;
import java.util.function.IntUnaryOperator;
import java.util.function.LongUnaryOperator;

/**
 * Operations on a single field that can be used in an {@link OperationTransformBuilder}
 * to create an {@link DidoTransform}.
 */
public class BasicOperations {

    /**
     * Provide fluent copy builders with the same structure. An {@link CopyField}
     * can then define different operations.
     *
     * @param <O> The Operation that will be doing the copy.
     */
    private interface CopyOpFactory<O> {

        O with(CopyTo<O> to);
    }

    /**
     * Specify the copy from. Either the field name or the field index can be specified.
     *
     * @param <O> The Operation Type passed through to the {@link CopyTo#with()}
     */
    public static class CopyField<O> {

        private final CopyOpFactory<O> opFactory;

        private String from;

        private CopyField(CopyOpFactory<O> opFactory) {
            this.opFactory = opFactory;
        }

        /**
         * The name of the field to copy from.
         *
         * @param name A field name. Must exist in the schema. Must not be null.
         * @return An ongoing fluent copy builder to specify the 'to' field.
         */
        public CopyTo<O> from(String name) {
            Objects.requireNonNull(name, "Field name can not be null");
            this.from = name;
            return new CopyTo<>(this);
        }
    }

    /**
     * Fluent Builder to specify the 'to' options for a copy operation.
     *
     * @param <O> The type of copy operation.
     */
    public static class CopyTo<O> {

        private final CopyOpFactory<O> opFactory;

        private final String from;

        private String to;

        private CopyTo(CopyField<O> copyField) {
            this.opFactory = copyField.opFactory;
            this.from = Objects.requireNonNull(copyField.from, "No from");
        }

        /**
         * The name of the field to copy to. If the name exists the field will be replaced.
         *
         * @param to The field name.
         * @return these fluent options.
         */
        public CopyTo<O> to(String to) {
            this.to = to;
            return this;
        }

        /**
         * Continue to the final stage of building a copy operation.
         *
         * @return The operation specification.
         */
        public O with() {
            return opFactory.with(this);
        }

        String to() {
            return to == null ? from : to;
        }

        @Override
        public String toString() {
            return "Copy from " + from +
                    " to " + to();
        }
    }

    /**
     * Builder for the definition of a simple copy.
     */
    public static class CopyDef {

        private final CopyTo<?> copyTo;

        private Type type;

        private DidoConversionProvider conversionProvider;

        public CopyDef(CopyTo<?> copyTo) {
            this.copyTo = copyTo;
        }

        /**
         * Define a new type for the copied field, which may involve a conversion.
         *
         * @param type The type.
         * @return This builder for more options.
         */
        public CopyDef type(Type type) {
            this.type = type;
            return this;
        }

        /**
         * Provide a Conversion Provider that will be used to convert the value to the type.
         *
         * @param conversionProvider A Conversion Provider.
         * @return This builder for more options.
         */
        public CopyDef conversionProvider(DidoConversionProvider conversionProvider) {
            this.conversionProvider = conversionProvider;
            return this;
        }

        /**
         * Create the copy as a view over the incoming data.
         *
         * @return The view.
         */
        public OperationDefinition op() {

            return new OperationDefinition() {

                @Override
                public Runnable prepare(OperationContext context) {

                    ValueGetter getter = context.getterNamed(copyTo.from);
                    String to = copyTo.to();
                    if (type == null) {
                        ValueSetter setter = context.setterNamed(to, getter.getType());
                        return copyOpFor(getter, setter, getter.getType());
                    } else {
                        Function<?, ?> conversion = Objects.requireNonNullElse(conversionProvider,
                                        DefaultConversionProvider.defaultInstance())
                                .conversionFor(getter.getType(), type);
                        ValueSetter setter = context.setterNamed(to, type);
                        return () -> {
                            if (getter.has()) {
                                //noinspection rawtypes,unchecked
                                setter.set(((Function) conversion).apply(getter.get()));
                            } else {
                                setter.clear();
                            }
                        };
                    }
                }

                @Override
                public String toString() {
                    return copyTo.toString();
                }
            };
        }
    }

    static class CopyDefFactory implements CopyOpFactory<CopyDef> {

        @Override
        public CopyDef with(CopyTo<CopyDef> to) {
            return new CopyDef(to);
        }
    }

    /**
     * @param getter The getter.
     * @param setter The setter.
     * @param type  The type.
     * @return The op.
     */
    static Runnable copyOpFor(ValueGetter getter,
                              ValueSetter setter,
                              Type type) {

        if (boolean.class == type) {
            return () -> {
                if (getter.has()) {
                    setter.setBoolean(getter.getBoolean());
                } else {
                    setter.clear();
                }
            };
        } else if (byte.class == type) {
            return () -> {
                if (getter.has()) {
                    setter.setByte(getter.getByte());
                } else {
                    setter.clear();
                }
            };
        } else if (short.class == type) {
            return () -> {
                if (getter.has()) {
                    setter.setShort(getter.getShort());
                } else {
                    setter.clear();
                }
            };
        } else if (char.class == type) {
            return () -> {
                if (getter.has()) {
                    setter.setChar(getter.getChar());
                } else {
                    setter.clear();
                }
            };
        } else if (int.class == type) {
            return () -> {
                if (getter.has()) {
                    setter.setInt(getter.getInt());
                } else {
                    setter.clear();
                }
            };
        } else if (long.class == type) {
            return () -> {
                if (getter.has()) {
                    setter.setLong(getter.getLong());
                } else {
                    setter.clear();
                }
            };
        } else if (float.class == type) {
            return () -> {
                if (getter.has()) {
                    setter.setFloat(getter.getFloat());
                } else {
                    setter.clear();
                }
            };
        } else if (double.class == type) {
            return () -> {
                if (getter.has()) {
                    setter.setDouble(getter.getDouble());
                } else {
                    setter.clear();
                }
            };
        } else {
            return () -> setter.set(getter.get());
        }
    }

    /**
     * Create a copy operation using fluent field locations.
     *
     * @return Fluent fields to define the copy.
     */
    public static CopyField<CopyDef> copy() {
        return new CopyField<>(new CopyDefFactory());
    }

    /**
     * Create an operation that copies the named field. The field will be copied to the same index
     * in the resultant schema.
     *
     * @param name The field name to copy.
     * @return A Copy Operation Definition.
     */
    public static OperationDefinition copyNamed(String name) {

        return copy().from(name)
                .with().op();
    }

    /**
     * Create an operation that copies the named field to another name. The field will be copied to the same
     * index in the resultant schema.
     *
     * @param from The field name to copy.
     * @param to   The field name to copy.
     * @return A Copy Operation Definition.
     */
    public static OperationDefinition copyNamed(String from, String to) {

        return copy().from(from)
                .to(to)
                .with().op();
    }

    /**
     * Rename a field.
     *
     * @param from The existing field name.
     * @param to   The new field name.
     * @return An operation definition.
     */
    public static OperationDefinition rename(String from, String to) {

        return (context) -> {

            Runnable copy = copy()
                    .from(Objects.requireNonNull(from, "No From"))
                    .to(Objects.requireNonNull(to, "No To"))
                    .with().op().prepare(context);

            context.removeNamed(from);

            return copy;
        };
    }

    /**
     * Provide fluent setting builders with the same structure. An {@link SetField}
     * can then define different operations.
     *
     * @param <O> The Operation that will be doing the setting.
     */
    private interface SetOpFactory<O> {

        O with(SetField<O> set);
    }

    /**
     * Specify the set location operation. Either the field name or the field index can be specified.
     *
     * @param <O> The Operation Type passed through to the {@link CopyTo#with()}
     */
    public static class SetField<O> {

        private final SetOpFactory<O> opFactory;

        private String name;

        private SetField(SetOpFactory<O> opFactory) {
            this.opFactory = opFactory;
        }

        /**
         * The name of the field to set the value at.
         *
         * @param name The field name. If it exists the existing value will be overwritten.
         * @return These field setting options.
         */
        public SetField<O> named(String name) {
            this.name = name;
            return this;
        }

        /**
         * Continue to the final stage of building a set operation.
         *
         * @return The operation specification.
         */
        public O with() {
            return opFactory.with(this);
        }

        @Override
        public String toString() {
            return "Set" +
                    (name == null ? "" : " named " + name);
        }
    }

    static class SetValueFactory implements SetOpFactory<SetValue> {

        @Override
        public SetValue with(SetField<SetValue> setField) {
            return new SetValue(setField);
        }
    }

    /**
     * Builder for an operation to set a field to a constant value. The field may have a name, or an index, or both.
     * If the index is 0, the new field will be added to the schema, if it is negative the existing index
     * is used if it exists.
     * Specifying a type is useful when the new field is to be a primitive type, or a super class of the value.
     * If the value is not assignable to the type a conversion is used.
     */
    public static class SetValue {

        private final SetField<?> setField;

        private Object value;

        private Type type;

        private DidoConversionProvider conversionProvider;

        SetValue(SetField<?> setField) {
            this.setField = setField;
        }

        /**
         * Provide the value to set.
         *
         * @param value The value.
         * @return This builder for more options.
         */
        public SetValue value(Object value) {
            this.value = value;
            return this;
        }

        /**
         * Specify The type of the value.
         *
         * @param type The type.
         * @return This builder for more options.
         */
        public SetValue type(Type type) {
            this.type = type;
            return this;
        }

        /**
         * Provide a Conversion Provider that will be used to convert the value to the type.
         *
         * @param conversionProvider A Conversion Provider.
         * @return This builder for more options.
         */
        public SetValue conversionProvider(DidoConversionProvider conversionProvider) {
            this.conversionProvider = conversionProvider;
            return this;
        }

        /**
         * Create the set as a view over the incoming data.
         *
         * @return The view.
         */
        public OperationDefinition op() {

            Type valueType = value == null ? void.class : value.getClass();

            return new OperationDefinition() {


                Object convertedValue(Type type) {

                    if (value == null) {
                        return null;
                    } else {
                        DidoConversionProvider conversionProviderOrDefault = Objects.requireNonNullElse(
                                conversionProvider,
                                DefaultConversionProvider.defaultInstance());

                        return conversionProviderOrDefault.conversionFor(value.getClass(), type).apply(value);
                    }
                }

                @Override
                public Runnable prepare(OperationContext context) {

                    if (type == null) {
                        Type newType = Objects.requireNonNullElse(context.typeOfNamed(setField.name), valueType);
                        ValueSetter setter = context.setterNamed(setField.name, newType);
                        return setterFactoryFor(setter, value, newType);
                    } else {
                        Object convertedValue = convertedValue(valueType);
                        ValueSetter setter = context.setterNamed(setField.name, type);
                        return setterFactoryFor(setter, convertedValue, type);
                    }
                }

                @Override
                public String toString() {
                    return setField.toString();
                }
            };
        }
    }

    /**
     * Create a set operation using fluent field locations.
     *
     * @return Fluent fields to define the set.
     */
    public static SetField<SetValue> set() {
        return new SetField<>(new SetValueFactory());
    }


    /**
     * Create an operation to set a field with the given name to be the given value. If the field of this name
     * exists the field index will be preserved.
     *
     * @param name  The name of the field.
     * @param value The value to set.
     * @return The Operation Definition.
     */
    public static OperationDefinition setNamed(String name,
                                               Object value) {
        return set()
                .named(name)
                .with()
                .value(value)
                .op();
    }

    /**
     * Create an operation to set a field with the given name to be the given value, with a schema type
     * of the given type. If the field of this name exists the field index will be preserved. Specifying a type
     * is useful when the new field is to be a primitive type, or a super class of the value. No check is made that the
     * value is assignable to the type.
     *
     * @param name  The name of the field.
     * @param value The value to set.
     * @param type  The type of the field.
     * @return The Operation Definition.
     */
    public static OperationDefinition setNamed(String name,
                                               Object value,
                                               Class<?> type) {

        return set()
                .named(name)
                .with()
                .value(value)
                .type(type)
                .op();
    }

    /**
     * @param value The value to set.
     * @param type  The type.
     * @return The prepare step.
     */
    static Runnable setterFactoryFor(ValueSetter setter,
                                     Object value,
                                     Type type) {

        if (value == null) {
            return setter::clear;
        } else if (boolean.class == type) {
            boolean boolValue = (boolean) value;
            return () -> setter.setBoolean(boolValue);
        } else if (byte.class == type) {
            byte byteValue = (byte) value;
            return () -> setter.setByte(byteValue);
        } else if (short.class == type) {
            short shortValue = (short) value;
            return () -> setter.setShort(shortValue);
        } else if (char.class == type) {
            char charValue = (char) value;
            return () -> setter.setChar(charValue);
        } else if (int.class == type) {
            int intValue = (int) value;
            return () -> setter.setInt(intValue);
        } else if (long.class == type) {
            long longValue = (long) value;
            return () -> setter.setLong(longValue);
        } else if (float.class == type) {
            float floatValue = (float) value;
            return () -> setter.setFloat(floatValue);
        } else if (double.class == type) {
            double doubleValue = (double) value;
            return () -> setter.setDouble(doubleValue);
        } else {
            return () -> setter.set(value);
        }
    }

    /**
     * Remove a field by name.
     *
     * @param name The name of the field.
     * @return The operation definition.
     */
    public static OperationDefinition removeNamed(String name) {

        return context -> {
            context.removeNamed(name);
            return () -> {};
        };
    }

    /**
     * Remove a field by index.
     *
     * @param index The index of the field.
     * @return The operation definition.
     */
    public static FieldView removeAt(int index) {

        return (incomingSchema, viewDefinition) -> {
            SchemaField field = incomingSchema.getSchemaFieldAt(index);
            if (field == null) {
                throw new NoSuchFieldException(index, incomingSchema);
            }
            viewDefinition.removeField(field);
        };
    }

    public static class FuncMapDef {

        private final CopyTo<?> copyTo;

        private final Class<?> type;

        FuncMapDef(CopyTo<?> copyTo) {
            this(copyTo, null);
        }

        FuncMapDef(CopyTo<?> copyTo,
                   Class<?> type) {
            this.copyTo = copyTo;
            this.type = type;
        }

        /**
         * Define a new type for the resultant field.
         *
         * @param type The type.
         * @return Ongoing mapping definition.
         */
        public FuncMapDef type(Class<?> type) {
            return new FuncMapDef(copyTo, type);
        }

        /**
         * Apply a mapping function.
         *
         * @param func The function.
         * @return The op
         */
        public OperationDefinition func(Function<?, ?> func) {

            return new OperationDefinition() {

                @Override
                public Runnable prepare(OperationContext context) {

                    ValueGetter getter = context.getterNamed(copyTo.from);
                    String to = copyTo.to();
                    ValueSetter setter;
                    if (type == null) {
                        setter = context.setterNamed(to, getter.getType());
                    } else {
                        setter = context.setterNamed(to, type);
                    }

                    return () -> {
                        if (getter.has()) {
                            //noinspection rawtypes,unchecked
                            setter.set(((Function) func).apply(getter.get()));
                        } else {
                            setter.clear();
                        }
                    };
                }

                @Override
                public String toString() {
                    return copyTo + " with Function " + func;
                }
            };
        }

        /**
         * Apply a unary int operation.
         *
         * @param func The operation.
         * @return The op
         */
        public OperationDefinition intOp(IntUnaryOperator func) {

            return new OperationDefinition() {

                @Override
                public Runnable prepare(OperationContext context) {

                    ValueGetter getter = context.getterNamed(copyTo.from);
                    ValueSetter setter = context.setterNamed(copyTo.to(), int.class);

                    return () -> {
                        if (getter.has()) {
                            setter.setInt(func.applyAsInt(getter.getInt()));
                        } else {
                            setter.clear();
                        }
                    };
                }
            };
        }

        /**
         * Apply a unary long operation.
         *
         * @param func The operation.
         * @return The op
         */
        public OperationDefinition longOp(LongUnaryOperator func) {

            return new OperationDefinition() {

                @Override
                public Runnable prepare(OperationContext context) {

                    ValueGetter getter = context.getterNamed(copyTo.from);
                    ValueSetter setter = context.setterNamed(copyTo.to(), long.class);

                    return () -> {
                        if (getter.has()) {
                            setter.setLong(func.applyAsLong(getter.getLong()));
                        } else {
                            setter.clear();
                        }
                    };
                }
            };
        }

        public OperationDefinition doubleOp(DoubleUnaryOperator func) {

            return new OperationDefinition() {

                @Override
                public Runnable prepare(OperationContext context) {

                    ValueGetter getter = context.getterNamed(copyTo.from);
                    ValueSetter setter = context.setterNamed(copyTo.to(), double.class);

                    return () -> {
                        if (getter.has()) {
                            setter.setDouble(func.applyAsDouble(getter.getDouble()));
                        } else {
                            setter.clear();
                        }
                    };
                }
            };
        }
    }

    public static class FuncMapDefFactory implements CopyOpFactory<FuncMapDef> {

        @Override
        public FuncMapDef with(CopyTo<FuncMapDef> to) {
            return new FuncMapDef(to);
        }
    }

    /**
     * Create an operation to copy a field applying a function
     * using fluent field locations.
     *
     * @return fluent fields to define the copy.
     */
    public static CopyField<FuncMapDef> map() {
        return new CopyField<>(new FuncMapDefFactory());
    }


}
