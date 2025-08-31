package dido.flow;

import dido.data.DidoData;

public interface DidoSubscriber {

    void onData(DidoData data);

    void onPartial(DidoData partial);

    void onDelete(DidoData keyData);

}
