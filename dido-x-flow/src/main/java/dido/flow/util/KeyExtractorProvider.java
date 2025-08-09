package dido.flow.util;

import dido.data.DataSchema;

public interface KeyExtractorProvider<K extends Comparable<K>> {

    KeyExtractor<K> keyExtractorFor(DataSchema schema);
}
