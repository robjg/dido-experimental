package dido.elsewhere.ema;

import com.refinitiv.ema.access.*;
import com.refinitiv.ema.rdm.DataDictionary;
import com.refinitiv.ema.rdm.EmaRdm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

class DictionaryConsumerClient implements OmmConsumerClient, AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(DictionaryConsumerClient.class);

    private final DataDictionary dataDictionary;

    private final Consumer<? super DataDictionary> onDictionary;

    private boolean fldDictComplete = false;
    private boolean enumTypeComplete = false;

    DictionaryConsumerClient(DataDictionary dataDictionary,
                             Consumer<? super DataDictionary> onDictionary) {
        this.dataDictionary = dataDictionary;
        this.onDictionary = onDictionary;
    }


    @Override
    public void close() {

    }

    @Override
    public void onRefreshMsg(RefreshMsg refreshMsg, OmmConsumerEvent consumerEvent) {

        logger.debug("onRefreshMsg: {}", refreshMsg);

        int dataType = refreshMsg.payload().dataType();

        if (dataType == DataType.DataTypes.SERIES) {

            if (refreshMsg.name().equals("RWFFld")) {
                dataDictionary.decodeFieldDictionary(refreshMsg.payload().series(), EmaRdm.DICTIONARY_NORMAL);
                fldDictComplete = true;
            } else if (refreshMsg.name().equals("RWFEnum")) {
                dataDictionary.decodeEnumTypeDictionary(refreshMsg.payload().series(), EmaRdm.DICTIONARY_NORMAL);
                enumTypeComplete = true;
            }

            if (fldDictComplete && enumTypeComplete) {
                onDictionary.accept(dataDictionary);
            }

        } else {
            logger.warn("Unsupported data type: {}", dataType);
        }
    }

    @Override
    public void onUpdateMsg(UpdateMsg updateMsg, OmmConsumerEvent consumerEvent) {
        logger.warn("UNEXPECTED onUpdateMsg: {}", updateMsg);
    }

    @Override
    public void onStatusMsg(StatusMsg statusMsg, OmmConsumerEvent consumerEvent) {
        logger.warn("UNEXPECTED onStatusMsg: {}", statusMsg);
    }

    @Override
    public void onGenericMsg(GenericMsg genericMsg, OmmConsumerEvent consumerEvent) {
        logger.warn("UNEXPECTED onGenericMsg: {}", genericMsg);
    }

    @Override
    public void onAckMsg(AckMsg ackMsg, OmmConsumerEvent consumerEvent) {
        logger.warn("UNEXPECTED onAckMsg: {}", ackMsg);
    }

    @Override
    public void onAllMsg(Msg msg, OmmConsumerEvent consumerEvent) {
        // Ignore - already processed.
    }
}
