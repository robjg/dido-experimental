package dido.table.internal;

import dido.operators.transform.FieldReader;
import dido.table.LiveValue;
import dido.table.QuietlyCloseable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

public class ObjectLiveValue implements LiveValue {

    private final List<Consumer<? super FieldReader>> listeners = new ArrayList<>();

    boolean changed;

    private Object value;

    @Override
    public boolean has() {
        return value != null;
    }

    @Override
    public Object get() {
        return value;
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

    @Override
    public void clear() {
        if (value != null) {
            changed = true;
        }
        set(null);
    }

    @Override
    public void set(Object value) {
        if (Objects.equals(this.value, value)) {
            changed = false;
        }
        else {
            changed = true;
            this.value = value;
            fireChange();
        }
    }

    @Override
    public void setBoolean(boolean value) {
        set(value);
    }

    @Override
    public void setByte(byte value) {
        set(value);
    }

    @Override
    public void setChar(char value) {
        set(value);
    }

    @Override
    public void setShort(short value) {
        set(value);
    }

    @Override
    public void setInt(int value) {
        set(value);
    }

    @Override
    public void setLong(long value) {
        set(value);
    }

    @Override
    public void setFloat(float value) {
        set(value);
    }

    @Override
    public void setDouble(double value) {
        set(value);
    }

    @Override
    public void setString(String value) {
        set(value);
    }

    @Override
    public QuietlyCloseable onChange(Consumer<? super FieldReader> listener) {
        listener.accept(this);
        listeners.add(listener);
        return () -> listeners.remove(listener);
    }

    protected void fireChange() {
        listeners.forEach(listener -> listener.accept(this));
    }
}
