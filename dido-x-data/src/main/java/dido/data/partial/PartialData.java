package dido.data.partial;

import dido.data.DidoData;
import dido.data.util.FieldSelectionFactory;

import java.util.Objects;

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

    /**
     * Provide a standard way of calculating the hash code.
     *
     * @param partial The partial data.
     * @return The hash code.
     */
    static int hashCode(PartialData partial) {
        int hash = 0;
        for (int index = partial.firstIndex(); index > 0; index = partial.nextIndex(index)) {
            Object value = partial.getData().getAt(index);
            hash = hash * 31 + (value == null ? 0 :value.hashCode());
        }
        return hash;
    }

    /**
     * Provide a standard way of testing equality. Partial Dido Data depends on
     * iteration order not on being the same set of fields.
     *
     * @param partial1 The first data.
     * @param partial2 The second data.
     *
     * @return true if they are equal. false otherwise.
     */
    static boolean equals(PartialData partial1, PartialData partial2) {
        if (partial1 == partial2) {
            return true;
        }
        if (partial1 == null || partial2 == null) {
            return false;
        }

        int index1 = partial1.firstIndex(), index2 = partial2.firstIndex();
        for ( ; index1 > 0 && index2 > 0; index1 = partial1.nextIndex(index1), index2 = partial2.nextIndex(index2)) {
            if (! Objects.equals(partial1.getData().getAt(index1), partial2.getData().getAt(index2))) {
                return false;
            }
        }
        return index1 == 0 && index2 == 0;
    }
}
