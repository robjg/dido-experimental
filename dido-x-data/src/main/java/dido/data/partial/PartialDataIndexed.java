package dido.data.partial;

import dido.data.DidoData;

public class PartialDataIndexed extends AbstractPartialData implements PartialData {

    private final DidoData data;

    private final int[] modifiedIndices;

    private int lastPos;

    private PartialDataIndexed(DidoData data, int[] modifiedIndices) {
        this.data = data;
        this.modifiedIndices = modifiedIndices;
    }

    public static PartialData of(DidoData data, int... modifiedIndices) {
        if (modifiedIndices.length == 0) {
            return new Empty(data);
        }
        if (modifiedIndices.length == 1) {
            return new Single(data, modifiedIndices[0]);
        }
        return new PartialDataIndexed(data, modifiedIndices.clone());
    }

    @Override
    public DidoData getData() {
        return data;
    }

    @Override
    public int firstIndex() {
        lastPos = 0;
        return modifiedIndices[0];
    }

    @Override
    public int nextIndex(int index) {
        int pos = lastPos;
        if (modifiedIndices[pos] == index) {
            if (pos == modifiedIndices.length - 1) {
                lastPos = 0;
                return 0;
            }
            else {
                lastPos = pos++;
                return modifiedIndices[pos];
            }
        }
        else {
            for (pos = 0; pos < modifiedIndices.length - 1; ++pos) {
                if (modifiedIndices[pos] == index) {
                    ++pos;
                    lastPos = pos;
                    return modifiedIndices[pos];
                }
            }
        }
        lastPos = 0;
        return 0;
    }

    @Override
    public int lastIndex() {
        return modifiedIndices[modifiedIndices.length -1];
    }

    @Override
    public IndexIterator indexIterator() {
        return IndexIterator.of(modifiedIndices);
    }

    @Override
    public int getSize() {
        return modifiedIndices.length;
    }

    @Override
    public int[] getIndices() {
        return modifiedIndices.clone();
    }

    static class Empty extends AbstractPartialData {

        private final DidoData data;

        Empty(DidoData data) {
            this.data = data;
        }

        @Override
        public DidoData getData() {
            return data;
        }

        @Override
        public int firstIndex() {
            return 0;
        }

        @Override
        public int nextIndex(int index) {
            return 0;
        }

        @Override
        public int lastIndex() {
            return 0;
        }

        @Override
        public int getSize() {
            return 0;
        }

        @Override
        public int[] getIndices() {
            return new int[0];
        }
    }

    static class Single extends AbstractPartialData {

        private final DidoData data;

        private final int index;

        Single(DidoData data,
               int index) {
            this.data = data;
            this.index = index;
        }

        @Override
        public DidoData getData() {
            return data;
        }

        @Override
        public int firstIndex() {
            return index;
        }

        @Override
        public int nextIndex(int index) {
            return 0;
        }

        @Override
        public int lastIndex() {
            return index;
        }

        @Override
        public int getSize() {
            return 1;
        }

        @Override
        public int[] getIndices() {
            return new int[] { index };
        }
    }
}
