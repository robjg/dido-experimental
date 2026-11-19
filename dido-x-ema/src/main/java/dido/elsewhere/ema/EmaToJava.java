package dido.elsewhere.ema;

import com.refinitiv.ema.access.DataType;
import com.refinitiv.ema.access.FieldEntry;
import com.refinitiv.eta.codec.DataTypes;

import java.util.function.Function;
import java.util.function.ToLongFunction;

public class EmaToJava {

    public static Function<FieldEntry, String> stringExtractor(int rwfType) {

        return switch(rwfType) {
            case DataTypes.ASCII_STRING -> fe -> fe.ascii().ascii();
            case DataTypes.UTF8_STRING -> fe -> fe.utf8().buffer().toString();
            case DataTypes.RMTES_STRING -> fe -> fe.rmtes().rmtes().toString();
            default -> null;
        };
    }

    public static ToLongFunction<FieldEntry> longExtractor(int rwfType) {

        return switch(rwfType) {
            case DataType.DataTypes.REAL -> fe -> fe.real().mantissa();
            case DataType.DataTypes.INT -> FieldEntry::intValue;
            case DataType.DataTypes.UINT -> FieldEntry::uintValue;
            default -> null;
        };
    }

}
