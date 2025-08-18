package dido.operators.transform;

import java.util.Objects;

public abstract class AbstractValueGetter implements ValueGetter {

    @Override
    public boolean has() {
        return get() != null;
    }

    @Override
    public boolean getBoolean() {
        return (boolean) get();
    }

    @Override
    public char getChar() {
        return (char) get();
    }

    @Override
    public byte getByte() {
        return ((Number) get()).byteValue();
    }

    @Override
    public short getShort() {
        return ((Number) get()).shortValue();
    }

    @Override
    public int getInt() {
        return ((Number) get()).intValue();
    }

    @Override
    public long getLong() {
        return ((Number) get()).longValue();
    }

    @Override
    public float getFloat() {
        return ((Number) get()).floatValue();
    }

    @Override
    public double getDouble() {
        return ((Number) get()).doubleValue();
    }

    @Override
    public String getString() {
        return Objects.toString(get());
    }
}
