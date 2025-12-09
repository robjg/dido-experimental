package dido.data.partial;

import dido.data.DidoData;
import dido.data.util.FieldSelectionFactory;

public interface PartialUpdate extends IndexSequence {

    DidoData getData();

    static FieldSelectionFactory<PartialUpdate> from(DidoData data) {
        return new FieldSelectionFactory<>(data.getSchema(), ints -> PartialUpdateIndexed.of(data, ints));
    }

    static PartialUpdate of(DidoData data) {
        return new PartialUpdateOf(data);
    }

    static String toString(PartialUpdate partial) {
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
