package dido.flow.util;

import dido.data.DataSchema;
import dido.data.DidoData;
import dido.data.FieldGetter;
import dido.data.ReadSchema;

import java.util.function.Function;

public class KeyExtractors {

    static class Impl implements KeyExtractor {

        private final Function<? super DidoData, ? extends Comparable<?>> func;

        private Impl(Function<? super DidoData, ? extends Comparable<?>> func) {
            this.func = func;
        }

        @Override
        public Comparable<?> keyOf(DidoData data) {
            return func.apply(data);        }
    }

    public static KeyExtractor fromFirstField(DataSchema schema) {
        int index = schema.firstIndex();
        if (index < 1) {
            throw new IllegalArgumentException("No First Field");
        }
        ReadSchema readSchema = ReadSchema.from(schema);
        FieldGetter getter = readSchema.getFieldGetterAt(index);
        return new Impl(data -> (Comparable<?>) getter.get(data));
    }


}
