package dido.operators.transform;

import java.lang.reflect.Type;
import java.util.Objects;

public interface ValueGetter {

    Type getType();

    Object get();

    default boolean has() {
        return get() != null;
    }

    default boolean getBoolean() {
        return (boolean) get();
    }

    default char getChar() {
        return (char) get();
    }

    default byte getByte() {
        return ((Number) get()).byteValue();
    }

    default int getInt() {
        return ((Number) get()).intValue();
    }

    default short getShort() {
        return ((Number) get()).shortValue();
    }

    default long getLong() {
        return ((Number) get()).longValue();
    }

    default float getFloat() {
        return ((Number) get()).floatValue();
    }

    default double getDouble() {
        return ((Number) get()).doubleValue();
    }

    default String getString() {
        return Objects.toString(get());
    }
}
