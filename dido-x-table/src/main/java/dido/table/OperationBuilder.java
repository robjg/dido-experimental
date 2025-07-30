package dido.table;

import java.util.function.Consumer;

public interface OperationBuilder {

    OperationBuilder readingNamed(String name);

    OperationBuilder settingNamed(String name);

    Consumer<LiveRow> processor(Object processor);

}
