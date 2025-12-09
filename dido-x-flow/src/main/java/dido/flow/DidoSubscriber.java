package dido.flow;

import dido.data.DidoData;
import dido.data.partial.PartialUpdate;

public interface DidoSubscriber {

    void onData(DidoData data);

    void onPartial(PartialUpdate partial);

    void onDelete(DidoData keyData);

}
