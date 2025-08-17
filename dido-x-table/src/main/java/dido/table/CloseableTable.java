package dido.table;

import dido.flow.QuietlyCloseable;

public interface CloseableTable<K extends Comparable<K>> extends DataTable<K>, QuietlyCloseable  {


}
