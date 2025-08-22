package dido.table;

import dido.data.DidoData;
import dido.flow.DidoSubscriber;

public interface LiveTable extends DidoSubscriber, AutoCloseable {

    LiveRow getRow(DidoData data);

    @Override
    void close();
}
