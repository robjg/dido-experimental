package dido.elsewhere.ema;

import com.refinitiv.ema.access.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ChainClientConsumer implements OmmConsumerClient {

    private static final Logger logger = LoggerFactory.getLogger(ChainClientConsumer.class);

    private final ReqMsg reqMsg = EmaFactory.createReqMsg();

    private final OmmConsumer consumer;

    private final String serviceName;

    private final LegacyChainConsumer chainConsumer;

    ChainClientConsumer(OmmConsumer consumer, String serviceName,
                        LegacyChainConsumer chainConsumer) {
        this.consumer = consumer;
        this.serviceName = serviceName;
        this.chainConsumer = chainConsumer;
    }

    void subscribeChain(String chainName) {

        long handle = consumer.registerClient(reqMsg.clear()
                        .serviceName(serviceName)
                        .name(chainName),
                this);
    }


    @Override
    public void onRefreshMsg(RefreshMsg refreshMsg, OmmConsumerEvent consumerEvent) {

        logger.debug("onRefreshMsg: {}", refreshMsg);

        int dataType = refreshMsg.payload().dataType();

        if (dataType == DataType.DataTypes.FIELD_LIST) {
            FieldList fieldList = refreshMsg.payload().fieldList();
            chainConsumer.onChain(refreshMsg.name(), fieldList,
                    this::subscribeChain);
        }
        else {
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
