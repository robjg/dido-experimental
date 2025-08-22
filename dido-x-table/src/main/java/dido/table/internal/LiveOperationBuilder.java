package dido.table.internal;

import dido.data.DataSchema;
import dido.data.NoSuchFieldException;
import dido.data.SchemaFactory;
import dido.data.SchemaField;
import dido.operators.transform.*;
import dido.table.LiveRow;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * Create an operation by providing an {@link dido.operators.transform.OperationContext}
 * for a {@link dido.table.LiveTable}.
 */
public class LiveOperationBuilder {

    private final SchemaFactory schemaFactory;

    private final List<OperationDefinition> opDefs = new ArrayList<>();

    private LiveOperationBuilder(SchemaFactory schemaFactory) {
        this.schemaFactory = schemaFactory;
    }

    public static LiveOperationBuilder forSchema(DataSchema incomingSchema) {
        return new LiveOperationBuilder(SchemaFactory.newInstanceFrom(incomingSchema));
    }

    static class RowManager implements Consumer<LiveRow> {

        private LiveRow row;

        @Override
        public void accept(LiveRow row) {
            this.row = row;
        }

        ValueGetter createValueGetter(Type type, int index) {

            return new ValueGetter() {

                @Override
                public Type getType() {
                    return type;
                }

                @Override
                public Object get() {
                    return row.getValueAt(index).get();
                }
            };
        }


        ValueSetter createValueSetter(Type type, int index) {

            return new ValueSetter() {


                @Override
                public Type getType() {
                    return null;
                }

                @Override
                public void set(Object value) {
                    row.getValueAt(index).set(value);
                }
            };
        }
    }

    public LiveOperationBuilder addOp(OperationDefinition opDef) {
        this.opDefs.add(opDef);
        return this;
    }

    public LiveOperation build() {

        RowManager rowManager = new RowManager();

        class LiveOperationContext implements OperationContext {

            @Override
            public Type typeOfNamed(String name) {
                return schemaFactory.getTypeNamed(name);
            }

            @Override
            public ValueGetter getterNamed(String name) {
                int index = schemaFactory.getIndexNamed(name);
                if (index == 0) {
                    throw new NoSuchFieldException(name, schemaFactory);
                }
                return rowManager.createValueGetter(schemaFactory.getTypeAt(index), index);
            }

            @Override
            public ValueSetter setterNamed(String name, Type type) {
                int index = schemaFactory.getIndexNamed(name);
                if (index == 0) {
                    index = schemaFactory.addSchemaField(SchemaField.of(0, name, type))
                            .getIndex();
                }
                return rowManager.createValueSetter(schemaFactory.getTypeAt(index), index);
            }

            @Override
            public void removeNamed(String name) {
                schemaFactory.removeNamed(name);
            }
        }

        OperationContext context = new LiveOperationContext();

        List<Runnable> operations = opDefs.stream()
                .map(od -> od.prepare(context))
                .toList();

        DataSchema schema = schemaFactory.toSchema();

        return new LiveOperation() {
            @Override
            public DataSchema getResultantSchema() {
                return schema;
            }

            @Override
            public void accept(LiveRow row) {
                rowManager.accept(row);
                operations.forEach(Runnable::run);
            }
        };
    }

}
