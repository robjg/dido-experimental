package dido.flow;

import dido.data.DidoData;
import dido.data.partial.PartialData;

public interface DidoSubscriber {

    void onData(DidoData data);

    void onPartial(PartialData partial);

    void onDelete(DidoData keyData);

}
