package dido.operators.transform;

import java.lang.reflect.Type;

public interface OperationContext {

    Type typeOfNamed(String name);

    ValueGetter getterNamed(String name);

    ValueSetter setterNamed(String name, Type type);

    void removeNamed(String name);
}
