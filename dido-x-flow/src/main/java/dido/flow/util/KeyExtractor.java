package dido.flow.util;

import dido.data.DidoData;

public interface KeyExtractor {

    Comparable<?> keyOf(DidoData data);

}
