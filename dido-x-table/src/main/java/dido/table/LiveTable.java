package dido.table;

import dido.data.DidoData;
import dido.flow.Receiver;

public interface LiveTable extends Receiver, AutoCloseable {

    LiveRow getRow(DidoData data);

    @Override
    void close();
}
