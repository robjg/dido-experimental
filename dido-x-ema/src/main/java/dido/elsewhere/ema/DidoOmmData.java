package dido.elsewhere.ema;

import com.refinitiv.ema.access.*;
import dido.data.*;
import dido.data.NoSuchFieldException;
import dido.data.partial.PartialUpdate;
import dido.data.schema.DataSchemaImpl;
import dido.data.useful.AbstractData;
import dido.data.useful.AbstractFieldGetter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;
import java.util.Map;

public class DidoOmmData  {

    private static final Logger logger = LoggerFactory.getLogger(DidoOmmData.class);

    private final Schema schema;

    private final Map<Integer, Integer> fidMap;

    DidoOmmData(Schema schema, Map<Integer, Integer> fidMap) {
        this.schema = schema;
        this.fidMap = fidMap;
    }



    public static DidoOmmData of(FieldList fieldEntries) {

        int size = fieldEntries.size();
        List<SchemaField> schemaFields = new ArrayList<>();
        Map<Integer, Integer> fidMap = new HashMap<>();
        FieldGetter[] getters = new FieldGetter[size];
        int i = 0;
        for (FieldEntry fieldEntry : fieldEntries) {

            SchemaField schemaField = schemaField(fieldEntry, i);
            if (schemaField == null) {
                throw new IllegalArgumentException("Unrecognized type for Fid: " + fieldEntry.fieldId() +
                        " Name = " + fieldEntry.name() + " DataType: " +
                        DataType.asString(fieldEntry.load().dataType()) + " Value: ");

            }
            schemaFields.add(schemaField);
            getters[i] = fieldGetter(fieldEntry.loadType(), i);
            ++i;
            fidMap.put(fieldEntry.fieldId(), i);
        }

        return new DidoOmmData(new Schema(schemaFields, 1, size, getters),
                fidMap);
    }

    public DidoData data(FieldList fieldEntries) {

        int size = fieldEntries.size();
        FieldEntry[] entries = new FieldEntry[size];
        int i = 0;
        for (FieldEntry fieldEntry : fieldEntries) {

            entries[i++] = fieldEntry;
        }

        return data(entries);
    }

    public DidoData data(FieldEntry[] entries) {
        return new Data(entries);
    }

    public PartialUpdate partial(FieldList fieldEntries) {

        int size = fieldEntries.size();
        FieldEntry[] entries = new FieldEntry[size];
        int[] indices = new int[size];
        int i = 0;
        for (FieldEntry fieldEntry : fieldEntries) {
            indices[i] = fidMap.get(fieldEntry.fieldId());
            entries[i++] = fieldEntry;
        }

        DidoData data = data(entries);

        return PartialUpdate.from(data).withIndices(indices);
    }

    static int nanoSecondsFrom(int milliSeconds, int microSeconds, int nanoSeconds) {
        return milliSeconds * 1_000_000 + microSeconds * 1_000 + nanoSeconds;
    }


    abstract static class OmmFieldGetter extends AbstractFieldGetter {

        protected final int index;

        OmmFieldGetter(int index) {
            this.index = index;
        }
    }

    static class Schema extends DataSchemaImpl implements ReadSchema {

        private final FieldGetter[] getters;

        Schema(Collection<SchemaField> fields,
               int firstIndex,
               int lastIndex,
               FieldGetter[] getters) {

            super(fields, firstIndex, lastIndex);
            this.getters = getters;
        }

        @Override
        public FieldGetter getFieldGetterAt(int index) {
            try {
                FieldGetter getter = getters[index - 1];
                if (getter == null) {
                    throw new NoSuchFieldException(index, Schema.this);
                }
                return getter;
            } catch (ArrayIndexOutOfBoundsException e) {
                throw new NoSuchFieldException(index, Schema.this);
            }
        }

        @Override
        public FieldGetter getFieldGetterNamed(String name) {
            int index = getIndexNamed(name);
            if (index == 0) {
                throw new NoSuchFieldException(name, Schema.this);
            }
            return getters[index - 1];
        }
    }

    class Data extends AbstractData {

        private final FieldEntry[] fields;

        Data(FieldEntry[] fields) {
            this.fields = fields;
        }

        @Override
        public DataSchema getSchema() {
            return schema;
        }

        @Override
        public Object getAt(int index) {
            return schema.getters[index -1].get(this);
        }


    }



    static SchemaField schemaField(FieldEntry fieldEntry, int index) {

        Class<?> type = clasFor(fieldEntry.loadType());
        if (type == null) {
            return null;
        }
        String name = fieldEntry.name();
        if (name == null || name.isEmpty()) {
            name = "fid_" + fieldEntry.fieldId();
        }
        return SchemaField.of(index, name, type);
    }

    static Class<?> clasFor(int dataType) {

        return switch (dataType) {
            case DataType.DataTypes.REAL -> double.class;
            case DataType.DataTypes.DATE -> LocalDate.class;
            case DataType.DataTypes.TIME -> LocalTime.class;
            case DataType.DataTypes.DATETIME -> LocalDateTime.class;
            case DataType.DataTypes.INT, DataType.DataTypes.UINT -> long.class;
            case DataType.DataTypes.ASCII, DataType.DataTypes.ERROR -> String.class;
            case DataType.DataTypes.ENUM -> int.class;
            case DataType.DataTypes.RMTES -> ByteBuffer.class;
            default -> null;
        };
    }

    static FieldGetter fieldGetter(int dataType, int index) {

        return switch (dataType) {
            case DataType.DataTypes.REAL -> new RealGetter(index);
            case DataType.DataTypes.DATE -> new DateGetter(index);
            case DataType.DataTypes.TIME -> new TimeGetter(index);
            case DataType.DataTypes.DATETIME -> new DateTimeGetter(index);
            case DataType.DataTypes.INT -> new IntGetter(index);
            case DataType.DataTypes.UINT -> new UintGetter(index);
            case DataType.DataTypes.ASCII -> new AsciiGetter(index);
            case DataType.DataTypes.ERROR -> new ErrorGetter(index);
            case DataType.DataTypes.ENUM -> new EnumGetter(index);
            case DataType.DataTypes.RMTES -> new RmtesGetter(index);
            default -> null;
        };
    }

    static class Blank extends OmmFieldGetter {

        Blank(int index) {
            super(index);
        }

        @Override
        public Object get(DidoData data) {
            return null;
        }
    }

    static class RealGetter extends OmmFieldGetter {

        RealGetter(int index) {
            super(index);
        }

        @Override
        public Object get(DidoData data) {
            return getDouble(data);
        }

        @Override
        public double getDouble(DidoData data) {
            return ((Data) data).fields[index].doubleValue();
        }
    }

    static class DateGetter extends OmmFieldGetter {

        DateGetter(int index) {
            super(index);
        }

        @Override
        public Object get(DidoData data) {

            OmmDate date = ((Data) data).fields[index].date();
            return LocalDate.of(date.year(), date.month(), date.day());
        }
    }

    static class TimeGetter extends OmmFieldGetter {

        TimeGetter(int index) {
            super(index);
        }

        @Override
        public Object get(DidoData data) {

            OmmTime time = ((Data) data).fields[index].time();
            return LocalTime.of(time.hour(), time.minute(), time.second(),
                    nanoSecondsFrom(time.millisecond(), time.microsecond(), time.nanosecond()));
        }
    }

    static class DateTimeGetter extends OmmFieldGetter {

        DateTimeGetter(int index) {
            super(index);
        }

        @Override
        public Object get(DidoData data) {

            OmmDateTime dateTime = ((Data) data).fields[index].dateTime();
            return LocalDateTime.of(dateTime.year(), dateTime.month(), dateTime.day(),
                    dateTime.hour(), dateTime.minute(), dateTime.second(),
                    nanoSecondsFrom(dateTime.millisecond(), dateTime.microsecond(), dateTime.nanosecond()));
        }
    }


    static class IntGetter extends OmmFieldGetter {

        IntGetter(int index) {
            super(index);
        }

        @Override
        public Object get(DidoData data) {
            return getLong(data);
        }

        @Override
        public long getLong(DidoData data) {
            return ((Data) data).fields[index].intValue();
        }
    }

    static class UintGetter extends OmmFieldGetter {

        UintGetter(int index) {
            super(index);
        }

        @Override
        public Object get(DidoData data) {
            return getLong(data);
        }

        @Override
        public long getLong(DidoData data) {
            return ((Data) data).fields[index].uintValue();
        }
    }

    static class AsciiGetter extends OmmFieldGetter {

        AsciiGetter(int index) {
            super(index);
        }

        @Override
        public Object get(DidoData data) {
            return getString(data);
        }

        @Override
        public String getString(DidoData data) {
            return ((Data) data).fields[index].ascii().ascii();
        }
    }

    static class EnumGetter extends OmmFieldGetter {

        EnumGetter(int index) {
            super(index);
        }

        @Override
        public Object get(DidoData data) {
            return getInt(data);
        }

        @Override
        public int getInt(DidoData data) {
            return ((Data) data).fields[index].enumValue();
        }
    }

    static class RmtesGetter extends OmmFieldGetter {

        RmtesGetter(int index) {
            super(index);
        }

        @Override
        public Object get(DidoData data) {
            return ((Data) data).fields[index].rmtes().rmtes().asUTF8();
        }

    }

    static class ErrorGetter extends OmmFieldGetter {

        ErrorGetter(int index) {
            super(index);
        }

        @Override
        public Object get(DidoData data) {
            return getString(data);
        }

        @Override
        public String getString(DidoData data) {
            return ((Data) data).fields[index].error().errorCodeAsString();
        }
    }
}

