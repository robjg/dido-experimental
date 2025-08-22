package dido.operators.transform;

import java.lang.reflect.Type;

public interface ValueSetter {

    Type getType();

    void set(Object value);

    default void clear() {
        set(null);
    }

    default void setBoolean(boolean value) {
        set(value);
    }

    default void setByte(byte value) {
        set(value);
    }

    default void setChar(char value) {
        set(value);
    }

    default void setShort(short value) {
        set(value);
    }

    default void setInt(int value) {
        set(value);
    }

    default void setLong(long value) {
        set(value);
    }

    default void setFloat(float value) {
        set(value);
    }

    default void setDouble(double value) {
        set(value);
    }

    default void setString(String value) {
        set(value);
    }
}
