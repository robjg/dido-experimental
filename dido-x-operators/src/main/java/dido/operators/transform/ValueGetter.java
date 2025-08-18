package dido.operators.transform;

import java.lang.reflect.Type;

public interface ValueGetter<T> {

    Type getType();

    boolean has();

    T get();

}
