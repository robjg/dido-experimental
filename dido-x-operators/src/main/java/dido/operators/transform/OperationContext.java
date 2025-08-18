package dido.operators.transform;

import java.lang.reflect.Type;

public interface OperationContext {

    ValueGetter getterNamed(String name);

    ValueSetter writeNamed(String name, Type type);
}
