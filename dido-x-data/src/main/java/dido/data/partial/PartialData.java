package dido.data.partial;

import dido.data.DidoData;
import dido.data.util.FieldSelectionFactory;

public interface PartialData extends IndexSequence {

    DidoData getData();

    static FieldSelectionFactory<PartialData> from(DidoData data) {
        return new FieldSelectionFactory<>(data.getSchema(), ints -> PartialDataIndexed.of(data, ints));
    }

    static PartialData of(DidoData data, int... indices) {
        if (indices.length == 0) {
            return new PartialDataOf(data);
        }
        else {
            return PartialDataIndexed.of(data, indices);
        }
    }

    static String toString(PartialData partial) {
        StringBuilder sb = new StringBuilder(partial.lastIndex() * 16);
        sb.append('{');
        for (int index = partial.firstIndex(); index > 0; index = partial.nextIndex(index)) {
            sb.append('[');
            String field = partial.getData().getSchema().getFieldNameAt(index);
            sb.append(index);
            if (field != null) {
                sb.append(':');
                sb.append(field);
            }
            sb.append("]=");
            sb.append(partial.getData().getAt(index));
            if (index != partial.lastIndex()) {
                sb.append(", ");
            }
        }
        sb.append('}');
        return sb.toString();
    }
}
