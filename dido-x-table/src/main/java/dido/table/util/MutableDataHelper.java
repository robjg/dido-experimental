package dido.table.util;

import dido.data.DataSchema;
import dido.data.DidoData;
import dido.data.FromValues;
import dido.data.WriteSchema;
import dido.data.mutable.MutableArrayData;

import java.util.function.BiFunction;
import java.util.function.Function;

public class MutableDataHelper {

    private final WriteSchema writeSchema;

    private final Function<DidoData, DidoData> copyFunction;

    private final BiFunction<DidoData, MutableArrayData, int[]> updateFunction;

    private MutableDataHelper(WriteSchema writeSchema,
                              Function<DidoData, DidoData> copyFunction,
                              BiFunction<DidoData, MutableArrayData, int[]> updateFunction) {
        this.writeSchema = writeSchema;
        this.copyFunction = copyFunction;
        this.updateFunction = updateFunction;
    }

    public static MutableDataHelper forSchema(DataSchema fromSchema) {

        WriteSchema toSchema = MutableArrayData.asArrayDataSchema(fromSchema);

        BiFunction<DidoData, MutableArrayData, int[]> updateFunction = MutableArrayData
                .updateFunction(fromSchema);

        FromValues fromValues = MutableArrayData.withSchema(toSchema);

        return new MutableDataHelper(toSchema,
                fromValues.toCopyFunction(fromSchema), updateFunction);
    }

    public DataSchema getSchema() {
        return writeSchema;
    }

    public MutableArrayData copy(DidoData didoData) {

        return (MutableArrayData) copyFunction.apply(didoData);
    }

    public int[] update(DidoData fromData, MutableArrayData toData) {

        return updateFunction.apply(fromData, toData);
    }
}
