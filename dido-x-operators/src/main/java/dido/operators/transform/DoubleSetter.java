package dido.operators.transform;

public interface DoubleSetter extends ValueSetter<Double> {

    default void set(Double value) {
        if (value == null) {
            clear();
        }
        else {
            setDouble(value);
        }
    }

    void setDouble(double value);

}
