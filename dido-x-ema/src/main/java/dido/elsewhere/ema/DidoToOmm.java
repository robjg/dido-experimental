package dido.elsewhere.ema;

import com.refinitiv.ema.access.EmaFactory;
import com.refinitiv.ema.access.FieldEntry;
import com.refinitiv.ema.access.FieldList;
import com.refinitiv.ema.access.OmmReal;
import com.refinitiv.ema.rdm.DataDictionary;
import com.refinitiv.ema.rdm.DictionaryEntry;
import com.refinitiv.ema.rdm.MfFieldTypes;
import com.refinitiv.eta.codec.DataTypes;
import dido.data.*;
import dido.data.partial.PartialData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Provides a mapping of {@link DidoData} to an OMM {@link FieldList}.
 */
public class DidoToOmm implements Function<DidoData, FieldList> {

    private static final Logger logger = LoggerFactory.getLogger(DidoToOmm.class);

    private final Map<Integer, Function<DidoData, FieldEntry>> fieldEntries;

    private DidoToOmm(Map<Integer, Function<DidoData, FieldEntry>> fieldEntries) {
        this.fieldEntries = fieldEntries;
    }

    public static DidoToOmm forSchema(DataSchema schema, DataDictionary dictionary) {

        Map<Integer, Function<DidoData, FieldEntry>> fieldEntries =
                new LinkedHashMap<>();

        ReadSchema readSchema = ReadSchema.from(schema);

        for (SchemaField schemaField : schema.getSchemaFields()) {

            Type type = schemaField.getType();
            int index = schemaField.getIndex();

            DictionaryEntry dictionaryEntry = dictionary.entry(schemaField.getName());
            int fid = dictionaryEntry.fid();
            int rwfType = dictionaryEntry.rwfType();
            int fieldType = dictionaryEntry.fieldType();
            int length = dictionaryEntry.length();

            logger.debug("Creating Field for {}", dictionaryEntry);

            FieldGetter getter = readSchema.getFieldGetterAt(index);

            Function<DidoData, FieldEntry> fieldCreator = switch (rwfType) {
                case DataTypes.ASCII_STRING,
                     DataTypes.UTF8_STRING -> new AsciiField(fid, getter);
                case DataTypes.RMTES_STRING -> new RmtesField(fid, getter);
                case DataTypes.REAL -> fieldType == MfFieldTypes.INTEGER ?
                        new IntRealField(fid, getter) :
                        new RealField(fid, getter, OmmReal.MagnitudeType.EXPONENT_NEG_7);
                case DataTypes.DOUBLE,
                     DataTypes.FLOAT -> new DoubleField(fid, getter);
                case DataTypes.INT -> new IntField(fid, getter);
                case DataTypes.UINT -> new UIntField(fid, getter);
                default -> null;
            };

            if (fieldCreator == null) {
                logger.warn("No conversion from {} to an OMM field entry of {}.",
                        type.getTypeName(), rwfType);
            } else {
                fieldEntries.put(index, fieldCreator);
            }
        }

        return new DidoToOmm(fieldEntries);
    }

    @Override
    public FieldList apply(DidoData didoData) {
        FieldList fieldList = EmaFactory.createFieldList();
        for (Map.Entry<Integer, Function<DidoData, FieldEntry>> entry : fieldEntries.entrySet()) {
            Function<DidoData, FieldEntry> creator =  entry.getValue();
            fieldList.add(creator.apply(didoData));
        }
        return fieldList;
    }

    public Function<PartialData, FieldList> partialDataFunction() {

        return partialData -> {

            FieldList fieldList = EmaFactory.createFieldList();
            for (int i = partialData.firstIndex(); i != 0; i = partialData.nextIndex(i)) {
                Function<DidoData, FieldEntry> creator = fieldEntries.get(i);
                fieldList.add(creator.apply(partialData.getData()));
            }
            return fieldList;
        };
    }


    abstract static class FieldBase implements Function<DidoData, FieldEntry> {

        protected final FieldEntry fieldEntry = EmaFactory.createFieldEntry();

        protected final int fieldId;

        protected final FieldGetter getter;

        FieldBase(int fieldId, FieldGetter getter) {
            this.fieldId = fieldId;
            this.getter = getter;
        }

    }

    static class IntField extends FieldBase {

        IntField(int fieldId, FieldGetter getter) {
            super(fieldId, getter);
        }

        @Override
        public FieldEntry apply(DidoData data) {

            if (getter.has(data)) {
                return fieldEntry.intValue(fieldId, getter.getInt(data));
            } else
                return fieldEntry.codeInt(fieldId);
        }
    }

    static class UIntField extends FieldBase {

        UIntField(int fieldId, FieldGetter getter) {
            super(fieldId, getter);
        }

        @Override
        public FieldEntry apply(DidoData data) {

            if (getter.has(data)) {
                return fieldEntry.uintValue(fieldId, getter.getInt(data));
            } else
                return fieldEntry.codeUInt(fieldId);
        }
    }

    static class RealField extends FieldBase {

        private final int magnitude;

        RealField(int fieldId, FieldGetter getter, int magnitude) {
            super(fieldId, getter);
            this.magnitude = magnitude;
        }

        @Override
        public FieldEntry apply(DidoData data) {

            if (getter.has(data)) {
                return fieldEntry.realFromDouble(fieldId, getter.getDouble(data), magnitude);
            } else {
                return fieldEntry.codeReal(fieldId);
            }
        }
    }

    static class IntRealField extends FieldBase {

        IntRealField(int fieldId, FieldGetter getter) {
            super(fieldId, getter);
        }

        @Override
        public FieldEntry apply(DidoData data) {

            if (getter.has(data)) {
                return fieldEntry.real(fieldId, getter.getInt(data), OmmReal.MagnitudeType.EXPONENT_0);
            } else {
                return fieldEntry.codeReal(fieldId);
            }
        }
    }

    static class DoubleField extends FieldBase {

        DoubleField(int fieldId, FieldGetter getter) {
            super(fieldId, getter);
        }

        @Override
        public FieldEntry apply(DidoData data) {

            if (getter.has(data)) {
                return fieldEntry
                        .doubleValue(fieldId, getter.getDouble(data));
            } else {
                return fieldEntry.codeDouble(fieldId);
            }
        }
    }

    static class RmtesField extends FieldBase {

        RmtesField(int fieldId, FieldGetter getter) {
            super(fieldId, getter);
        }

        @Override
        public FieldEntry apply(DidoData data) {

            if (getter.has(data)) {
                return fieldEntry.rmtes(fieldId,
                        ByteBuffer.wrap(getter.getString(data).getBytes()));
            } else {
                return fieldEntry.codeRmtes(fieldId);
            }
        }
    }

    static class AsciiField implements Function<DidoData, FieldEntry> {

        private final int fieldId;

        private final FieldGetter getter;

        AsciiField(int fieldId, FieldGetter getter) {
            this.fieldId = fieldId;
            this.getter = getter;
        }

        @Override
        public FieldEntry apply(DidoData data) {

            return EmaFactory.createFieldEntry()
                    .ascii(fieldId, getter.getString(data));
        }
    }

}


