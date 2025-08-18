package dido.operators.transform;

import java.lang.reflect.Type;

public interface OperationContext {

    <T> ValueGetter<T> valueGetterNamed(String name);

    DoubleGetter doubleGetterNamed(String name);

    <T> ValueSetter<T> valueSetterNamed(String name, Type type);

    DoubleSetter doubleSetterNamed(String name);
}
