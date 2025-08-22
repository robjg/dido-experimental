package dido.table.internal;

import dido.operators.transform.OperationContext;
import dido.operators.transform.OperationDefinition;
import dido.operators.transform.ValueGetter;
import dido.operators.transform.ValueSetter;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Create an operation that invokes a method.
 */
public class MethodOperationBuilder {

    private final List<ParamMaker> paramMakers = new ArrayList<>();

    interface ParamMaker {

        Supplier<Object> toParam(OperationContext context);

    }

    record ReadParamMaker(String name) implements ParamMaker {

        @Override
        public Supplier<Object> toParam(OperationContext context) {
            ValueGetter getter = context.getterNamed(name);
            return getter::get;
        }
    }

    record WriteParamMaker(String name) implements ParamMaker {

        @Override
        public Supplier<Object> toParam(OperationContext context) {
            Type type = context.typeOfNamed(name);
            ValueSetter setter = context.setterNamed(name, type);
            return () -> (Consumer<Object>) setter::set;
        }
    }

    record WriteNewParamMaker(String name, Type type) implements ParamMaker {

        @Override
        public Supplier<Object> toParam(OperationContext context) {
            ValueSetter setter = context.setterNamed(name, type);
            return () -> (Consumer<Object>) setter::set;
        }
    }

    public MethodOperationBuilder readingNamed(String name) {
        paramMakers.add(new ReadParamMaker(name));
        return this;
    }

    public MethodOperationBuilder writingNamed(String name) {
        paramMakers.add(new WriteParamMaker(name));
        return this;
    }

    public MethodOperationBuilder writingNamed(String name, Type type) {
        paramMakers.add(new WriteNewParamMaker(name, type));
        return this;
    }

    public OperationDefinition processor(Object processor) {

        Method m = processor.getClass().getDeclaredMethods()[0];

        return context -> {

            List<Supplier<Object>> suppliers  = paramMakers.stream()
                    .map(pf -> pf.toParam(context))
                    .toList();

            return new MethodInvoker(m, processor, suppliers);
        };
    }

    static class MethodInvoker implements Runnable {

        private final Method method;

        private final Object object;

        private final List<Supplier<Object>> suppliers;

        MethodInvoker(Method method,
                      Object object, List<Supplier<Object>> suppliers) {
            this.method = method;
            this.object = object;
            this.suppliers = suppliers;
        }

        @Override
        public void run() {
            Object[] params = suppliers.stream()
                    .map(Supplier::get)
                    .toArray();
            try {
                method.invoke(object, params);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new RuntimeException(e);
            }
        }

    }
}
