package dido.elsewhere.ema;

import com.refinitiv.ema.access.*;
import com.refinitiv.ema.rdm.DataDictionary;
import com.refinitiv.ema.rdm.EmaRdm;
import dido.data.DidoData;
import dido.data.partial.PartialData;
import dido.flow.DidoSubscription;
import dido.flow.KeyedDataConsumer;
import dido.flow.QuietlyCloseable;
import dido.table.DataTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.*;
import java.util.Map;
import java.util.function.Function;

/**
 * Create an Omm Interactive Provider using the EMA library for providing
 * OMM MarketPrice price date to the Advanced Distribution Hub.
 */
public class DidoOmmIProvider implements OmmProviderClient {

    private static final Logger logger = LoggerFactory.getLogger(DidoOmmIProvider.class);

    private final Map<String, Set<Long>> handles = new HashMap<>();

    private final DidoToOmm didoToOmm;

    private final DataTable<String> dataTable;

    public DidoOmmIProvider(DidoToOmm didoToOmm,
                            DataTable<String> dataTable) {
        this.didoToOmm = didoToOmm;
        this.dataTable = dataTable;
    }

    public static class Settings {

        private String name;

        private int port;

        private Path dictionaryDir;

        public Settings port(int port) {
            this.port = port;
            return this;
        }

        public Settings dictionaryDir(Path dictionaryDir) {
            this.dictionaryDir = dictionaryDir;
            return this;
        }

        public QuietlyCloseable from(DataTable<String> dataTable) {

            Path dictionaryDir = Objects.requireNonNullElse(
                    this.dictionaryDir, Path.of("."));

                DataDictionary dictionary = EmaFactory.createDataDictionary();
                dictionary.loadFieldDictionary(dictionaryDir
                        .resolve("RDMFieldDictionary").toString());
                dictionary.loadEnumTypeDictionary(dictionaryDir
                        .resolve("enumtype.def").toString());

            DidoToOmm didoToOmm = DidoToOmm.forSchema(dataTable.getSchema(), dictionary);

            DidoOmmIProvider appClient = new DidoOmmIProvider(didoToOmm, dataTable);

            OmmIProviderConfig config = EmaFactory.createOmmIProviderConfig();

            logger.info("Creating config {}", config);

            String portStr = port == 0 ? "14002" : Integer.toString(port);

            logger.info("Creating provider on port {}", portStr);

            OmmProvider provider = EmaFactory.createOmmProvider(config.port(portStr), appClient);

            DidoSubscription subscription = appClient.init(provider);

            return () -> {
                subscription.close();
                provider.uninitialize();
            };
        }
    }

    public static Settings with() {
        return new Settings();
    }

    DidoSubscription init(OmmProvider provider) {
        return dataTable.subscribe(new DataForwarder(provider));
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
            handles.computeIfAbsent(key, k -> new HashSet<>())
                    .add(handle);
        }

        DidoData data = dataTable.get(key);

        if (data == null) {
            // TODO: What should we do here.
            logger.info("No data with key {}", key);
            return;
        }

        FieldList fieldList = didoToOmm.apply(data);

        logger.info("Processing Market Price request {}", reqMsg);

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


    class DataForwarder implements KeyedDataConsumer<String> {

        private final OmmProvider provider;

        private final Function<PartialData, FieldList> partialDataFunction =
                didoToOmm.partialDataFunction();

        DataForwarder(OmmProvider provider) {
            this.provider = provider;
        }

        @Override
        public void onData(String key, DidoData data) {

            Set<Long> clients = handles.get(key);
            if (clients == null || clients.isEmpty()) {
                return;
            }

            FieldList fieldList = didoToOmm.apply(data);

            send(clients, fieldList);
        }

        @Override
        public void onPartial(String key, PartialData partial) {

            Set<Long> clients = handles.get(key);
            if (clients == null || clients.isEmpty()) {
                return;
            }

            FieldList fieldList = partialDataFunction.apply(partial);

            send(clients, fieldList);
        }

        @Override
        public void onDelete(String key) {

        }

        void send(Set<Long> clients, FieldList fieldList) {

            UpdateMsg updateMsg = EmaFactory.createUpdateMsg().payload(fieldList);

            for (Long client : clients) {

                provider.submit( updateMsg, client);
            }
        }
    }


}
