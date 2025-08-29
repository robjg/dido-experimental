package dido.table.internal;

import dido.data.DataSchema;
import dido.table.LiveRow;

import java.util.function.Consumer;

public interface LiveOperation extends Consumer<LiveRow> {

    DataSchema getFullSchema();

    DataSchema getOutSchema();

}
