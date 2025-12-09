package dido.data.partial;

import dido.data.IndexedSchema;

public interface IndexSequence {

    int firstIndex();

    int nextIndex(int index);

    int lastIndex();

    int getSize();

    default int[] getIndices() {
        int[] indices = new int[getSize()];
        int i = 0;
        for (int index = firstIndex(); index > 0; index = nextIndex(index)) {
            indices[i++] = index;
        }
        return indices;
    }

    static IndexSequence fromSchema(IndexedSchema schema) {

        return new IndexSequence() {
            @Override
            public int firstIndex() {
                return schema.firstIndex();
            }

            @Override
            public int nextIndex(int index) {
                return schema.nextIndex(index);
            }

            @Override
            public int lastIndex() {
                return schema.lastIndex();
            }

            @Override
            public int getSize() {
                return schema.getSize();
            }

            @Override
            public int[] getIndices() {
                return schema.getIndices();
            }
        };
    }

    default IndexSequence transpose(int by) {

        return new IndexSequence() {
            @Override
            public int firstIndex() {
                int i = IndexSequence.this.firstIndex();
                if (i == 0) {
                    return 0;
                }
                else {
                    return by + i;
                }
            }

            @Override
            public int nextIndex(int index) {
                int i = IndexSequence.this.nextIndex(index - by);
                if (i == 0) {
                    return 0;
                }
                else {
                    return i + by;
                }
            }

            @Override
            public int lastIndex() {
                int i = IndexSequence.this.lastIndex();
                if (i == 0) {
                    return 0;
                }
                else {
                    return by + i;
                }
            }

            @Override
            public int getSize() {
                return IndexSequence.this.getSize();
            }
        };
    }
}
