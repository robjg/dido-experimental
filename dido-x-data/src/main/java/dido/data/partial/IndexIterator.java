package dido.data.partial;

/**
 * Not sure how useful this is.
 */
public interface IndexIterator {

    int next();

    static IndexIterator of(int... indices) {

        return new IndexIterator() {
            int pos = 0;
            @Override
            public int next() {
                if (pos >= indices.length) {
                    return 0;
                }
                else {
                    return indices[pos++];
                }
            }
        };
    }
}
