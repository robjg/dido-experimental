package dido.operators.transform;

import java.lang.reflect.Type;

public interface DoubleGetter extends ValueGetter<Double> {

    @Override
    default Type getType() {
        return double.class;
    }

    @Override
    default Double get() {
        if (has()) {
            return getDouble();
        }
        else {
            return null;
        }
    }

    double getDouble();
}
