package dido.elsewhere.ema;

import com.refinitiv.ema.access.Data;
import com.refinitiv.ema.access.DataType;
import com.refinitiv.ema.access.FieldEntry;
import com.refinitiv.eta.codec.DataTypes;

import java.nio.ByteBuffer;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;
import java.util.function.ToLongFunction;

public class EmaToJava {

    public static Function<FieldEntry, String> stringExtractor(int rwfType) {

        Function<FieldEntry, String> noBlank = stringExtractorNoBlank(rwfType);
        if (noBlank == null) {
            return null;
        }

        return fe -> fe.code() == Data.DataCode.BLANK ? null : noBlank.apply(fe);
    }

    public static Function<FieldEntry, String> stringExtractorNoBlank(int rwfType) {

        return switch (rwfType) {
            case DataType.DataTypes.ASCII -> fe -> fe.ascii().ascii();
            case DataType.DataTypes.UTF8 -> fe -> fe.utf8().buffer().toString();
            case DataType.DataTypes.RMTES -> fe -> fe.rmtes().rmtes().toString();
            default -> null;
        };
    }

    public static ToLongFunction<FieldEntry> longExtractor(int rwfType) {

        ToLongFunction<FieldEntry> noBlank = longExtractorNoBlank(rwfType);
        if (noBlank == null) {
            return null;
        }

        return fe -> fe.code() == Data.DataCode.BLANK ? 0L : noBlank.applyAsLong(fe);
    }

    public static ToLongFunction<FieldEntry> longExtractorNoBlank(int rwfType) {

        return switch (rwfType) {
            case DataType.DataTypes.REAL -> fe -> fe.real().mantissa();
            case DataType.DataTypes.INT -> FieldEntry::intValue;
            case DataType.DataTypes.UINT -> FieldEntry::uintValue;
            default -> null;
        };
    }

    public static ToDoubleFunction<FieldEntry> doubleExtractor(int rwfType) {

        ToDoubleFunction<FieldEntry> noBlank = doubleExtractorNoBlank(rwfType);
        if (noBlank == null) {
            return null;
        }

        return fe -> fe.code() == Data.DataCode.BLANK ? 0.0 : noBlank.applyAsDouble(fe);
    }

    public static ToDoubleFunction<FieldEntry> doubleExtractorNoBlank(int rwfType) {

        return switch (rwfType) {
            case DataType.DataTypes.REAL -> fe -> fe.real().asDouble();
            case DataType.DataTypes.FLOAT -> fe -> fe.ommFloatValue().floatValue();
            case DataType.DataTypes.DOUBLE -> fe -> fe.ommDoubleValue().doubleValue();
            default -> null;
        };
    }

    public static Function<FieldEntry, ByteBuffer> byteBufferExtractor(int rwfType) {

        Function<FieldEntry, ByteBuffer> noBlank = byteBufferExtractorNoBlank(rwfType);
        if (noBlank == null) {
            return null;
        }

        return fe -> fe.code() == Data.DataCode.BLANK ? null : noBlank.apply(fe);
    }

    public static Function<FieldEntry, ByteBuffer> byteBufferExtractorNoBlank(int rwfType) {

        return switch (rwfType) {
            case DataTypes.ASCII_STRING -> fe -> ByteBuffer.wrap(fe.ascii().ascii().getBytes());
            case DataTypes.UTF8_STRING -> fe -> fe.utf8().buffer();
            case DataTypes.RMTES_STRING -> fe -> fe.rmtes().rmtes().asUTF8();
            default -> null;
        };
    }

    public static Function<FieldEntry, CharSequence> charSequenceExtractor(int rwfType) {

        Function<FieldEntry, CharSequence> noBlank = charSequenceExtractorNoBlank(rwfType);
        if (noBlank == null) {
            return null;
        }

        return fe -> fe.code() == Data.DataCode.BLANK ? null : noBlank.apply(fe);
    }

    public static Function<FieldEntry, CharSequence> charSequenceExtractorNoBlank(int rwfType) {

        return switch (rwfType) {
            case DataTypes.ASCII_STRING -> fe -> fe.ascii().ascii();
            case DataTypes.UTF8_STRING -> fe -> fe.utf8().buffer().asCharBuffer();
            case DataTypes.RMTES_STRING -> fe -> fe.rmtes().rmtes().asUTF16();
            default -> null;
        };
    }

}
