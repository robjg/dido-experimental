package dido.data.partial;

import dido.data.DidoData;

import java.util.Objects;

public class PartialDataOf extends AbstractPartialData implements PartialData {

    private final DidoData data;

    PartialDataOf(DidoData data) {
        this.data = Objects.requireNonNull(data);
    }

    @Override
    public DidoData getData() {
        return data;
    }

    @Override
    public int firstIndex() {
        return data.getSchema().firstIndex();
    }

    @Override
    public int nextIndex(int index) {
        return data.getSchema().nextIndex(index);
    }

    @Override
    public int lastIndex() {
        return data.getSchema().lastIndex();
    }

    @Override
    public int getSize() {
        return data.getSchema().getSize();
    }
}
