package dido.table.util;

import dido.data.*;
import dido.data.mutable.MutableArrayData;

import java.util.function.BiFunction;

public class MutableDataHelper {

    private final DidoTransform copyFunction;

    private final BiFunction<DidoData, MutableArrayData, int[]> updateFunction;

    private MutableDataHelper(DidoTransform copyFunction,
                              BiFunction<DidoData, MutableArrayData, int[]> updateFunction) {
        this.copyFunction = copyFunction;
        this.updateFunction = updateFunction;
    }

    public static MutableDataHelper forSchema(DataSchema fromSchema) {

        WriteSchema toSchema = MutableArrayData.asArrayDataSchema(fromSchema);

        BiFunction<DidoData, MutableArrayData, int[]> updateFunction = MutableArrayData
                .updateFunction(fromSchema);

        FromValues fromValues = MutableArrayData.withSchema(toSchema);

        return new MutableDataHelper(fromValues.toCopyFunction(fromSchema),
                updateFunction);
    }

    public DataSchema getSchema() {
        return copyFunction.getSchema();
    }

    public MutableArrayData copy(DidoData didoData) {

        return (MutableArrayData) copyFunction.apply(didoData);
    }

    public int[] update(DidoData fromData, MutableArrayData toData) {

        return updateFunction.apply(fromData, toData);
    }
}
