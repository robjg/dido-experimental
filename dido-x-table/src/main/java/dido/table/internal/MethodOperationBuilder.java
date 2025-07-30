package dido.table.internal;

import dido.data.DataSchema;
import dido.data.NoSuchFieldException;
import dido.data.SchemaFactory;
import dido.data.SchemaField;
import dido.table.LiveRow;
import dido.table.OperationBuilder;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.IllformedLocaleException;
import java.util.List;
import java.util.function.Consumer;

/**
 * Create an operation that invokes a method.
 */
public class MethodOperationBuilder implements OperationBuilder {

    private final DataSchema schema;

    private final SchemaFactory schemaFactory;

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

    public MethodOperationBuilder(DataSchema schema, SchemaFactory schemaFactory) {
        this.schema = schema;
        this.schemaFactory = schemaFactory;
    }


    @Override
    public OperationBuilder readingNamed(String name) {
        int index = schema.getIndexNamed(name);
        if (index == 0) {
            throw new NoSuchFieldException(name, schema);
        }
        paramMakers.add(new ReadParamMaker(index));
        return this;
    }

    @Override
    public OperationBuilder writingNamed(String name) {
        int index = schema.getIndexNamed(name);
        if (index == 0) {
            throw new NoSuchFieldException(name, schema);
        }
        paramMakers.add(new WriteParamMaker(schema.getIndexNamed(name)));
        return this;
    }

    @Override
    public OperationBuilder writingNamed(String name, Type type) {
        int index = schema.getIndexNamed(name);
        if (index == 0) {
            index = schemaFactory.addSchemaField(SchemaField.of(0, name, type))
                    .getIndex();
        }
        else {
            throw new IllformedLocaleException("Field " + name + " exists already");
        }
        paramMakers.add(new WriteParamMaker(index));
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
