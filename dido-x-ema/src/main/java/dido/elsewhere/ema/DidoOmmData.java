package dido.elsewhere.ema;

import com.refinitiv.ema.access.*;
import com.refinitiv.ema.rdm.DataDictionary;
import com.refinitiv.ema.rdm.DictionaryEntry;
import com.refinitiv.ema.rdm.MfFieldTypes;
import dido.data.*;
import dido.data.NoSuchFieldException;
import dido.data.partial.PartialData;
import dido.data.schema.DataSchemaImpl;
import dido.data.schema.HasSchema;
import dido.data.useful.AbstractData;
import dido.data.useful.AbstractFieldGetter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Provide {@link DidoData} by wrapping OMM {@link FieldEntry}s.
 */
public class DidoOmmData implements HasSchema {

    private static final Logger logger = LoggerFactory.getLogger(DidoOmmData.class);

    private final Schema schema;

    private final Map<Integer, Integer> fidMap;

    DidoOmmData(Schema schema, Map<Integer, Integer> fidMap) {
        this.schema = schema;
        this.fidMap = fidMap;
    }

    public static class Settings {

        private DataSchema schema;

        private boolean partialSchema;

        public Settings schema(DataSchema schema) {
            this.schema = schema;
            this.partialSchema = false;
            return this;
        }

        public Settings partialSchema(DataSchema partialSchema) {
            this.schema = partialSchema;
            this.partialSchema = true;
            return this;
        }

        public Settings partialSchema(boolean partialSchema) {
            this.partialSchema = partialSchema;
            return this;
        }

        public DidoOmmData of(DataDictionary dictionary, int[] fids) {

            if (partialSchema || schema == null) {
                return ofUnknown(dictionary, fids);
            }
            else {
                return ofKnown(dictionary, Arrays.stream(fids)
                        .boxed()
                        .collect(Collectors.toSet()));
            }
        }

        public DidoOmmData ofUnknown(DataDictionary dictionary, int[] fids) {

            DataSchema schema = Objects.requireNonNullElseGet(this.schema,
                    DataSchema::emptySchema);

            int size = fids.length;
            List<SchemaField> schemaFields = new ArrayList<>();
            Map<Integer, Integer> fidMap = new HashMap<>(size);
            FieldGetter[] getters = new FieldGetter[size];
            for (int i = 0; i < size; ++i) {

                DictionaryEntry dictionaryEntry = dictionary.entry(fids[i]);

                int didoIndex = i + 1;

                SchemaField schemaField = schemaField(dictionaryEntry,
                        didoIndex, schema);

                getters[i] = fieldGetter(dictionaryEntry, i, schemaField.getType());

                schemaFields.add(schemaField);
                fidMap.put(dictionaryEntry.fid(), didoIndex);
            }

            return new DidoOmmData(
                    new Schema(schemaFields,
                            size == 0 ? 0 : 1, size, getters),
                    fidMap);
        }

        public DidoOmmData of(DataDictionary dictionary) {

            if (partialSchema || schema == null) {
                throw new IllegalArgumentException("PartialSchema or no schema so fields required.");
            }

            return ofKnown(dictionary, null);
        }

        private DidoOmmData ofKnown(DataDictionary dictionary, Set<Integer> fids) {

            List<SchemaField> schemaFields = new ArrayList<>();
            List<FieldGetter> getters = new ArrayList<>();
            Map<Integer, Integer> fidMap = new HashMap<>(schema.getSize());

            int i = 0;
            for (SchemaField schemaField : schema.getSchemaFields()) {
                String fieldName = schemaField.getName();
                if (!dictionary.hasEntry(fieldName)) {
                    continue;
                }
                DictionaryEntry dictionaryEntry = dictionary.entry(fieldName);
                int fid = dictionaryEntry.fid();
                if (fids != null && !fids.contains(fid)) {
                    continue;
                }

                getters.add(fieldGetter(dictionaryEntry, i++, schemaField.getType()));
                schemaFields.add(schemaField.mapToIndex(i));
                fidMap.put(fid, i);
            }

            return new DidoOmmData(new Schema(schemaFields,
                    i == 0 ? 0 : 1, i,
                    getters.toArray(new FieldGetter[0])),
                    fidMap);
        }

        public DidoOmmData of(DataDictionary dictionary, FieldList fieldList) {

            return of(dictionary,
                    fieldList.stream()
                            .map(FieldEntry::fieldId)
                            .mapToInt(Integer::intValue)
                            .toArray());
        }

    }

    public static Settings with() {
        return new Settings();
    }

    public static DidoOmmData of(DataDictionary dictionary, int[] fids) {

        return with().of(dictionary, fids);
    }

    public static DidoOmmData of(DataDictionary dictionary, FieldList fieldList) {

        return with().of(dictionary, fieldList);
    }

    @Override
    public DataSchema getSchema() {
        return schema;
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

    public PartialData partial(FieldList fieldEntries) {

        int size = fieldEntries.size();
        FieldEntry[] entries = new FieldEntry[schema.lastIndex()];
        int[] indices = new int[size];
        int i = 0;
        for (FieldEntry fieldEntry : fieldEntries) {
            int fieldIndex = fidMap.get(fieldEntry.fieldId());
            indices[i++] = fieldIndex;
            entries[fieldIndex - 1] = fieldEntry;
        }

        DidoData data = data(entries);

        return PartialData.from(data).withIndices(indices);
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
            return schema.getters[index - 1].get(this);
        }


    }

    static SchemaField schemaField(DictionaryEntry dictionaryEntry,
                                   int nextIndex,
                                   DataSchema schema) {



        String name = dictionaryEntry.acronym();
        SchemaField existing = schema.getSchemaFieldNamed(name);

        Type type;
        if (existing == null) {
                    type = classFor(dictionaryEntry);
                    if (type == null) {
                        throw new IllegalArgumentException("Unrecognized type for Fid: " + dictionaryEntry.fid() +
                                " Name = " + name + " DataType: " +
                                DataType.asString(dictionaryEntry.rwfType()) + " Value: ");
                    }
        } else {
            type = existing.getType();
        }

        return SchemaField.of(nextIndex, name, type);
    }

    static Class<?> classFor(DictionaryEntry dictionaryEntry) {

        int dataType = dictionaryEntry.rwfType();

        return switch (dataType) {
            case DataType.DataTypes.REAL -> dictionaryEntry.fieldType() == MfFieldTypes.INTEGER ?
                    long.class : double.class;
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

    static FieldGetter fieldGetter(DictionaryEntry dictionaryEntry,
                                   int arrayIndex,
                                   Type type) {

        int dataType = dictionaryEntry.rwfType();

        return switch (dataType) {
            case DataType.DataTypes.REAL -> dictionaryEntry.fieldType() == MfFieldTypes.INTEGER ?
                    type == long.class || type == Long.class ?
                    new RealLongGetter(arrayIndex) :
                    new RealIntGetter(arrayIndex) :
                    new RealGetter(arrayIndex);
            case DataType.DataTypes.DATE -> new DateGetter(arrayIndex);
            case DataType.DataTypes.TIME -> new TimeGetter(arrayIndex);
            case DataType.DataTypes.DATETIME -> new DateTimeGetter(arrayIndex);
            case DataType.DataTypes.INT -> new IntGetter(arrayIndex);
            case DataType.DataTypes.UINT -> new UintGetter(arrayIndex);
            case DataType.DataTypes.ASCII -> new AsciiGetter(arrayIndex);
            case DataType.DataTypes.ERROR -> new ErrorGetter(arrayIndex);
            case DataType.DataTypes.ENUM -> new EnumGetter(arrayIndex);
            case DataType.DataTypes.RMTES -> new RmtesGetter(arrayIndex);
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
            return ((Data) data).fields[index].real().asDouble();
        }
    }

    static class RealLongGetter extends OmmFieldGetter {

        RealLongGetter(int index) {
            super(index);
        }

        @Override
        public Object get(DidoData data) {
            return getLong(data);
        }

        @Override
        public long getLong(DidoData data) {
            return ((Data) data).fields[index].real().mantissa();
        }
    }

    static class RealIntGetter extends OmmFieldGetter {

        RealIntGetter(int index) {
            super(index);
        }

        @Override
        public Object get(DidoData data) {
            return getInt(data);
        }

        @Override
        public int getInt(DidoData data) {
            return (int) ((Data) data).fields[index].real().mantissa();
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
            return getString(data);
        }

        @Override
        public String getString(DidoData data) {
            return ((Data) data).fields[index].rmtes().rmtes().toString();
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

