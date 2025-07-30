package dido.table;

import dido.data.DataSchema;

public interface LiveRow {

    DataSchema getSchema();

    LiveValue getValueAt(int index);

    LiveValue getValueNamed(String name);
}
