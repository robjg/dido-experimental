package dido.elsewhere.ema;

import com.refinitiv.ema.access.EmaFactory;
import com.refinitiv.ema.access.FieldEntry;
import com.refinitiv.ema.access.FieldList;
import com.refinitiv.ema.rdm.DataDictionary;
import dido.data.*;

import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class DidoToOmm implements Function<DidoData, FieldList> {

    private final List<Function<DidoData, FieldEntry>> fieldEntries;

    private DidoToOmm(List<Function<DidoData, FieldEntry>> fieldEntries) {
        this.fieldEntries = fieldEntries;
    }

    public static class Factory {

        private final DataDictionary dictionary;

        private final DataSchema schema;


        public Factory(DataDictionary dictionary, DataSchema schema) {
            this.dictionary = dictionary;
            this.schema = schema;
        }
    }


    public static DidoToOmm forSchema(DataSchema schema) {

        List<Function<DidoData, FieldEntry>> fieldEntries =
                new ArrayList<>();

        ReadSchema readSchema = ReadSchema.from(schema);

        for (SchemaField schemaField : schema.getSchemaFields()) {

            Type type = schemaField.getType();
            int index = schemaField.getIndex();
            Function<DidoData, FieldEntry> setter;

            if (type == String.class) {
                    setter = new AsciiField(index,
                            readSchema.getFieldGetterAt(index));
            }
            else if (type == double.class | type == Double.class) {
                setter = new DoubleField(index,
                        readSchema.getFieldGetterAt(index));
            }
            else if (type == int.class | type == Integer.class) {
                setter = new IntegerField(index,
                        readSchema.getFieldGetterAt(index));
            }
            else {
                throw new IllegalArgumentException("No conversion from " + type.getTypeName() +
                        " to an OMM field entry.");
            }
            fieldEntries.add(setter);
        }

        return new DidoToOmm(fieldEntries);
    }

    @Override
    public FieldList apply(DidoData didoData) {
        FieldList fieldList = EmaFactory.createFieldList();
        for (Function<DidoData, FieldEntry> creator : fieldEntries) {
            fieldList.add(creator.apply(didoData));
        }
        return fieldList;
    }

    static class IntegerField implements Function<DidoData, FieldEntry> {

        private final int fieldId;

        private final FieldGetter getter;

        IntegerField(int fieldId, FieldGetter getter) {
            this.fieldId = fieldId;
            this.getter = getter;
        }

        @Override
        public FieldEntry apply(DidoData data) {

            return EmaFactory.createFieldEntry()
                    .intValue(fieldId, getter.getInt(data));
        }
    }

    static class DoubleField implements Function<DidoData, FieldEntry> {

        private final int fieldId;

        private final FieldGetter getter;

        DoubleField(int fieldId, FieldGetter getter) {
            this.fieldId = fieldId;
            this.getter = getter;
        }

        @Override
        public FieldEntry apply(DidoData data) {

            return EmaFactory.createFieldEntry()
                    .doubleValue(fieldId, getter.getDouble(data));
        }
    }

    static class StringField implements Function<DidoData, FieldEntry> {

        private final int fieldId;

        private final FieldGetter getter;

        StringField(int fieldId, FieldGetter getter) {
            this.fieldId = fieldId;
            this.getter = getter;
        }

        @Override
        public FieldEntry apply(DidoData data) {

            return EmaFactory.createFieldEntry()
                    .rmtes(fieldId, ByteBuffer.wrap(getter.getString(data).getBytes()));
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


