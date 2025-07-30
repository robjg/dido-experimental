package dido.table.internal;

import dido.data.DataSchema;
import dido.table.LiveRow;
import dido.table.OperationBuilder;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * Create an operation that invokes a method.
 */
public class MethodOperationBuilder implements OperationBuilder {

    private final DataSchema schema;

    private final List<ParamMaker> paramMakers = new ArrayList<>();

    interface ParamMaker {

        Object toParam(LiveRow row);

    }

    record ReadParamMaker(int index) implements ParamMaker {

        @Override
            public Object toParam(LiveRow row) {
                return row.getValueAt(index).get();
            }
        }

    record WriteParamMaker(int index) implements ParamMaker {

        @Override
            public Object toParam(LiveRow row) {
                return (Consumer<Object>) value -> row.getValueAt(index).set(value);
            }
        }

    public MethodOperationBuilder(DataSchema schema) {
        this.schema = schema;
    }


    @Override
    public OperationBuilder readingNamed(String name) {
        paramMakers.add(new ReadParamMaker(schema.getIndexNamed(name)));
        return this;
    }

    @Override
    public OperationBuilder settingNamed(String name) {
        paramMakers.add(new WriteParamMaker(schema.getIndexNamed(name)));
        return this;
    }

    @Override
    public Consumer<LiveRow> processor(Object processor) {

        Method m = processor.getClass().getDeclaredMethods()[0];

        return new MethodInvoker(m, processor);
    }

    class MethodInvoker implements Consumer<LiveRow> {

        private final Method method;

        private final Object object;

        MethodInvoker(Method method,
                      Object object) {
            this.method = method;
            this.object = object;
        }

        @Override
        public void accept(LiveRow row) {
            Object[] params = paramMakers.stream()
                    .map(pf -> pf.toParam(row))
                    .toArray();
            try {
                method.invoke(object, params);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new RuntimeException(e);
            }
        }

    }
}
