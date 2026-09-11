package dido.flow;

import dido.data.DidoData;
import dido.data.partial.PartialData;

public interface DidoDataConsumer {

    void onData(DidoData data);

    void onPartial(PartialData partial);

    void onDelete(DidoData keyData);

}
