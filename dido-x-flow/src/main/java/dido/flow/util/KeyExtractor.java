package dido.flow.util;

import dido.data.DidoData;

public interface KeyExtractor<K extends Comparable<K>> {

    K keyOf(DidoData data);

}
