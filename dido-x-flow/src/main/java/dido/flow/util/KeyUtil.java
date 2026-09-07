package dido.flow.util;

import dido.data.DataSchema;
import dido.data.DidoData;
import dido.data.FieldGetter;
import dido.data.ReadSchema;
import dido.data.immutable.SingleData;

import java.lang.reflect.Type;
import java.util.function.Function;

public class KeyUtil {


    public static <K> Function<DidoData, K> fromFirstField(DataSchema schema) {

        int index = schema.firstIndex();
        if (index < 1) {
            throw new IllegalArgumentException("No First Field");
        }
        ReadSchema readSchema = ReadSchema.from(schema);
        FieldGetter getter = readSchema.getFieldGetterAt(index);

        //noinspection unchecked
        return data -> (K) getter.get(data);
    }

    public static <K> Function<DidoData, K> fromNamed(DataSchema schema, String name) {

        ReadSchema readSchema = ReadSchema.from(schema);
        FieldGetter getter = readSchema.getFieldGetterNamed(name);

        //noinspection unchecked
        return data -> (K) getter.get(data);
    }

    public static <K> Function<K, DidoData> keyComposerOf(String name, Type type) {

        SingleData.ObjectType<K> singleDataType = SingleData.type(type);

        return singleDataType::of;
    }
}
