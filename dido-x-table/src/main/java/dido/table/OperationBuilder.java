package dido.table;

import java.lang.reflect.Type;
import java.util.function.Consumer;

public interface OperationBuilder {

    OperationBuilder readingNamed(String name);

    OperationBuilder writingNamed(String name);

    OperationBuilder writingNamed(String name, Type type);

    Consumer<LiveRow> processor(Object processor);

}
