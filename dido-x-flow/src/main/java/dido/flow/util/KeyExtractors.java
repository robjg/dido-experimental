package dido.flow.util;

import dido.data.DataSchema;
import dido.data.DidoData;
import dido.data.FieldGetter;
import dido.data.ReadSchema;

import java.util.function.Function;

public class KeyExtractors {

    static class Impl<K extends Comparable<K>> implements KeyExtractor<K> {

        private final Function<? super DidoData, ? extends K> func;

        private Impl(Function<? super DidoData, ? extends K> func) {
            this.func = func;
        }

        @Override
        public K keyOf(DidoData data) {
            return func.apply(data);        }
    }

    public static <K extends Comparable<K>> KeyExtractor<K> fromFirstField(DataSchema schema) {

        int index = schema.firstIndex();
        if (index < 1) {
            throw new IllegalArgumentException("No First Field");
        }
        ReadSchema readSchema = ReadSchema.from(schema);
        FieldGetter getter = readSchema.getFieldGetterAt(index);
        //noinspection unchecked
        return new Impl<>(data -> (K) getter.get(data));
    }


}
