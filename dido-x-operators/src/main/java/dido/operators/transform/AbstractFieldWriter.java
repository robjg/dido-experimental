package dido.operators.transform;

public abstract class AbstractFieldWriter implements FieldWriter {

    @Override
    public void clear() {
        set(null);
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

}
