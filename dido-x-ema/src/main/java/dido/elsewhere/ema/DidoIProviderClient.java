package dido.elsewhere.ema;

import com.refinitiv.ema.access.*;
import com.refinitiv.ema.rdm.EmaRdm;
import dido.data.DidoData;
import dido.data.partial.PartialUpdate;
import dido.flow.QuietlyCloseable;
import dido.table.DataTable;
import dido.table.KeyedSubscriber;
import dido.table.KeyedSubscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DidoIProviderClient implements OmmProviderClient {

    private static final Logger logger = LoggerFactory.getLogger(DidoIProviderClient.class);

    private final Map<String, Set<Long>> handles = new HashMap<>();

    private final DidoToOmm didoToOmm;

    private final DataTable<String> dataTable;

    public DidoIProviderClient(DidoToOmm didoToOmm,
                               DataTable<String> dataTable) {
        this.didoToOmm = didoToOmm;
        this.dataTable = dataTable;
    }

    public static class Settings {

        private String name;

        private int port;

        private DataTable<String> dataTable;

        public Settings port(int port) {
            this.port = port;
            return this;
        }

        public Settings dataTable(DataTable<String> dataTable) {
            this.dataTable = dataTable;
            return this;
        }

        public QuietlyCloseable create() {

            DidoToOmm didoToOmm = DidoToOmm.forSchema(dataTable.getSchema());

            DidoIProviderClient appClient = new DidoIProviderClient(didoToOmm, dataTable);
            KeyedSubscription subscription = appClient.init();

            OmmIProviderConfig config = EmaFactory.createOmmIProviderConfig();

            logger.info("Creating config {}", config);

            String portStr = port == 0 ? "14002" : Integer.toString(port);

            logger.info("Creating provider on port {}", portStr);

            OmmProvider provider = EmaFactory.createOmmProvider(config.port(portStr), appClient);

            return () -> {
                subscription.close();
                provider.uninitialize();
            };
        }
    }

    public static Settings with() {
        return new Settings();
    }

    KeyedSubscription init() {
        return dataTable.tableSubscribe(new DataForwarder());
    }

    public void onReqMsg(ReqMsg reqMsg, OmmProviderEvent event) {
        switch (reqMsg.domainType()) {
            case EmaRdm.MMT_LOGIN:
                processLoginRequest(reqMsg, event);
                break;
            case EmaRdm.MMT_MARKET_PRICE:
                processMarketPriceRequest(reqMsg, event);
                break;
            default:
                processInvalidItemRequest(reqMsg, event);
                break;
        }
    }

    public void onRefreshMsg(RefreshMsg refreshMsg, OmmProviderEvent event) {
    }

    public void onStatusMsg(StatusMsg statusMsg, OmmProviderEvent event) {
    }

    public void onGenericMsg(GenericMsg genericMsg, OmmProviderEvent event) {
    }

    public void onPostMsg(PostMsg postMsg, OmmProviderEvent event) {
    }

    public void onReissue(ReqMsg reqMsg, OmmProviderEvent event) {
    }

    public void onClose(ReqMsg reqMsg, OmmProviderEvent event) {
    }

    public void onAllMsg(Msg msg, OmmProviderEvent event) {
    }

    void processLoginRequest(ReqMsg reqMsg, OmmProviderEvent event) {
        event.provider()
                .submit(EmaFactory
                                .createRefreshMsg()
                                .domainType(EmaRdm.MMT_LOGIN)
                                .name(reqMsg.name())
                                .nameType(EmaRdm.USER_NAME)
                                .complete(true)
                                .solicited(true)
                                .state(OmmState.StreamState.OPEN,
                                        OmmState.DataState.OK,
                                        OmmState.StatusCode.NONE, "Login accepted"),
                        event.handle());
    }

    void processMarketPriceRequest(ReqMsg reqMsg, OmmProviderEvent event) {

        String key = reqMsg.name();

        Set<Long> handleSet = handles.get(key);

        long handle = event.handle();
        if (handleSet != null && !handleSet.contains(handle)) {
            processInvalidItemRequest(reqMsg, event);
            return;
        }
        else {
            handles.compute(key, (k, v) -> new HashSet<>()).add(handle);
        }

        DidoData data = dataTable.get(key);

        if (data == null) {
            // TODO: What should we do here.
            logger.info("No data with key {}", key);
            return;
        }

        FieldList fieldList = didoToOmm.apply(data);

        logger.info("Processing Market Price request {}", fieldList);

        event.provider().submit(EmaFactory.createRefreshMsg()
                        .name(key)
                        .serviceId(reqMsg.serviceId())
                        .solicited(true)
                        .state(OmmState.StreamState.OPEN, OmmState.DataState.OK, OmmState.StatusCode.NONE, "Refresh Completed")
                        .payload(fieldList)
                        .complete(true),
                handle);

    }

    void processInvalidItemRequest(ReqMsg reqMsg, OmmProviderEvent event) {
        event.provider().submit(EmaFactory.createStatusMsg().name(reqMsg.name()).serviceName(reqMsg.serviceName()).
                        state(OmmState.StreamState.CLOSED, OmmState.DataState.SUSPECT, OmmState.StatusCode.NOT_FOUND, "Item not found"),
                event.handle());
    }


    class DataForwarder implements KeyedSubscriber<String> {

        @Override
        public void onData(String key, DidoData data) {

            Set<Long> clients = handles.get(key);

            FieldList fieldList = didoToOmm.apply(data);

            UpdateMsg msg = EmaFactory.createUpdateMsg().payload(fieldList);

        }

        @Override
        public void onPartial(String key, PartialUpdate data) {

        }

        @Override
        public void onDelete(String key) {

        }
    }


}
