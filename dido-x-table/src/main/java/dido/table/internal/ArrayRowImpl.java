package dido.table.internal;

import dido.data.DataSchema;
import dido.data.DidoData;
import dido.data.NoSuchFieldException;
import dido.data.partial.IndexSequence;
import dido.data.partial.PartialUpdate;
import dido.data.useful.AbstractData;
import dido.flow.DidoSubscriber;
import dido.table.LiveRow;
import dido.table.LiveValue;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class ArrayRowImpl implements LiveRow {

    private final DataSchema schema;

    private final DidoSubscriber didoSubscriber;

    private final ObjectLiveValue[] values;

    public ArrayRowImpl(DataSchema schema,
                        DidoSubscriber didoSubscriber) {
        this.schema = schema;
        this.didoSubscriber = didoSubscriber;
        values = new ObjectLiveValue[schema.lastIndex()];
        for (int i = schema.firstIndex(); i > 0; i = schema.nextIndex(i)) {
            values[i -1] = new ObjectLiveValue();
        }
    }

    void onData(DidoData data, Consumer<LiveRow> ops) {
        load(IndexSequence.fromSchema(data.getSchema()), data, ops);

        boolean changed = false;
        for (ObjectLiveValue value : values) {
            if (value.changed) {
                changed = true;
                value.changed = false;
            }
        }
        if (changed && didoSubscriber != null) {
            didoSubscriber.onData(new RowData());
        }
    }

    public void onPartial(PartialUpdate partial, Consumer<LiveRow> ops) {
        load(partial, partial.getData(), ops);

        List<Integer> changed = new ArrayList<>();
        for (int i = 0; i < values.length; ++i) {
            if (values[i].changed) {
                changed.add(i + 1);
                values[i].changed = false;
            }
        }

        if (!changed.isEmpty() && didoSubscriber != null) {
            int[] ai = changed.stream().mapToInt(Integer::intValue).toArray();
            didoSubscriber.onPartial(PartialUpdate.from(new RowData()).withIndices(ai));
        }
    }

    public DidoData asData() {
        return new RowData();
    }

    void load(IndexSequence indices, DidoData data, Consumer<LiveRow> operations) {

        for (int index = indices.firstIndex(); index > 0; index = indices.nextIndex(index)) {

            if (schema.hasIndex(index)) {
                values[index - 1].set(data.getAt(index));
            }
        }
        operations.accept(this);

    }

    @Override
    public DataSchema getSchema() {
        return schema;
    }

    @Override
    public LiveValue getValueAt(int index) {
        try {
            LiveValue liveValue = values[index - 1];
            if (liveValue == null) {
                throw new NoSuchFieldException(index, schema);
            }
            return liveValue;
        }
        catch (IndexOutOfBoundsException e) {
            throw new NoSuchFieldException(index, schema);
        }
    }

    @Override
    public LiveValue getValueNamed(String name) {
        int index = schema.getIndexNamed(name);
        if (index == 0) {
            throw new NoSuchFieldException(name, schema);
        }
        return values[index -1];
    }

    class RowData extends AbstractData {

        @Override
        public DataSchema getSchema() {
            return schema;
        }

        @Override
        public Object getAt(int index) {
            return values[index -1].get();
        }
    }
}
