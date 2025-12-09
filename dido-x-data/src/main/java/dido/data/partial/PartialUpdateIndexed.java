package dido.data.partial;

import dido.data.DidoData;

public class PartialUpdateIndexed extends AbstractPartialUpdate implements PartialUpdate {

    private final DidoData data;

    private final int[] modifiedIndices;

    private int lastPos;

    private PartialUpdateIndexed(DidoData data, int[] modifiedIndices) {
        this.data = data;
        this.modifiedIndices = modifiedIndices;
    }

    public static PartialUpdate of(DidoData data, int... modifiedIndices) {
        return new PartialUpdateIndexed(data, modifiedIndices.clone());
    }

    @Override
    public DidoData getData() {
        return data;
    }

    @Override
    public int firstIndex() {
        if (modifiedIndices.length > 0) {
            lastPos = 0;
            return modifiedIndices[0];
        }
        else {
            return 0;
        }
    }

    @Override
    public int nextIndex(int index) {
        int pos = lastPos;
        if (pos == 0 || !(modifiedIndices[pos] == index)) {
            for (pos = 0; pos < modifiedIndices.length; ++pos) {
                if (modifiedIndices[pos] == index) {
                    break;
                }
            }
        }
        ++pos;
        if (pos == modifiedIndices.length) {
            lastPos = 0;
            return 0;
        }
        else {
            lastPos = pos;
            return modifiedIndices[pos];
        }
    }

    @Override
    public int lastIndex() {
        if (modifiedIndices.length > 0) {
            lastPos = modifiedIndices.length -1;
            return modifiedIndices[modifiedIndices.length -1];
        }
        else {
            lastPos = 0;
            return 0;
        }
    }

    @Override
    public int getSize() {
        return modifiedIndices.length;
    }

    @Override
    public int[] getIndices() {
        return modifiedIndices.clone();
    }

}
