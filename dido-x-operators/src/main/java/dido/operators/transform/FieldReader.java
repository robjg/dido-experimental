package dido.operators.transform;

public interface FieldReader {

    boolean has();

    Object get();

    boolean getBoolean();

    char getChar();

    byte getByte();

    short getShort();

    int getInt();

    long getLong();

    float getFloat();

    double getDouble();

    String getString();
}
