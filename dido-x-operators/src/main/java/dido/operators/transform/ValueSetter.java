package dido.operators.transform;

public interface ValueSetter {

    void clear();

    void set(Object value);

    void setBoolean(boolean value);

    void setByte(byte value);

    void setChar(char value);

    void setShort(short value);

    void setInt(int value);

    void setLong(long value);

    void setFloat(float value);

    void setDouble(double value);

    void setString(String value);
}
