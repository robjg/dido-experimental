package dido.flow;

import dido.data.DidoData;
import dido.data.partial.PartialData;

public interface Receiver extends AutoCloseable {

    void onData(DidoData data);

    void onPartial(PartialData partial);

    void onDelete(PartialData partial);

    @Override
    void close();
}
