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

;

/**
 * Create an Omm Interactive Provider using the EMA library for providing
 * OMM MarketPrice price date to the Advanced Distribution Hub.
 */
public class DidoOmmIProvider implements OmmProviderClient {

    private static final Logger logger = LoggerFactory.getLogger(DidoOmmIProvider.class);

    public static final int FRAGMENTATION_SIZE = 96000;

    private final Map<String, Set<Long>> handles = new HashMap<>();

    private final DataDictionary dataDictionary;

    private final DidoToOmm didoToOmm;

    private final RefreshMsg refreshMsg;

    private final DataTable<String> dataTable;

    private final DirectoryRequestProcessor directoryRequestProcessor;

    private final SymbolListProcessor symbolListProcessor;

    public DidoOmmIProvider(String serviceName,
                            DataDictionary dataDictionary,
                            DidoToOmm didoToOmm,
                            DataTable<String> dataTable) {
        this.dataDictionary = dataDictionary;
        this.didoToOmm = didoToOmm;
        this.dataTable = dataTable;
        this.refreshMsg = EmaFactory.createRefreshMsg();
        this.symbolListProcessor = new SymbolListProcessor("INDEX", serviceName, dataTable, refreshMsg);
        this.directoryRequestProcessor = new DirectoryRequestProcessor(serviceName);
    }

    public static class Settings {

        private int port;

        private Path configPath;

        private Path fieldDictionaryPath;

        private Path enumTypeDictionaryPath;

        private String serviceName;

        public Settings port(int port) {
            this.port = port;
            return this;
        }

        public Settings configPath(Path configPath) {
            this.configPath = configPath;
            return this;
        }

        public Settings serviceName(String serviceName) {
            this.serviceName = serviceName;
            return this;
        }

        public Settings fieldDictionaryPath(Path fieldDictionaryPath) {
            this.fieldDictionaryPath = fieldDictionaryPath;
            return this;
        }

        public Settings enumTypeDictionaryPath(Path enumTypeDictionaryPath) {
            this.enumTypeDictionaryPath = enumTypeDictionaryPath;
            return this;
        }

        public QuietlyCloseable from(DataTable<String> dataTable) {

            String serviceName = Objects.requireNonNullElse(this.serviceName, "DIRECT_FEED");

            Path dictionaryFile = Objects.requireNonNullElse(fieldDictionaryPath,
                    Path.of("./RDMFieldDictionary"));
            Path enumTypeFile = Objects.requireNonNullElse(enumTypeDictionaryPath,
                    Path.of("./enumtype.def"));
            DataDictionary dictionary = EmaFactory.createDataDictionary();
            logger.info("Loading Data Dictionary from {}", dictionaryFile);
            dictionary.loadFieldDictionary(dictionaryFile.toString());
            logger.info("Loading Enum Type Dictionary from {}", enumTypeFile);
            dictionary.loadEnumTypeDictionary(enumTypeFile.toString());

            DidoToOmm didoToOmm = DidoToOmm.forSchema(dataTable.getSchema(), dictionary);

            DidoOmmIProvider appClient = new DidoOmmIProvider(serviceName,
                    dictionary, didoToOmm, dataTable);

            OmmIProviderConfig config;
            if (configPath == null) {
                logger.info("Creating default OmmIProviderConfig");
                config = EmaFactory.createOmmIProviderConfig();
            }
            else {
                logger.info("Creating OmmIProviderConfig from {}", configPath);
                config = EmaFactory.createOmmIProviderConfig(configPath.toString());
            }

            config = config
                    .adminControlDirectory(OmmIProviderConfig.AdminControl.USER_CONTROL)
                    .adminControlDictionary(OmmIProviderConfig.AdminControl.USER_CONTROL);

            if (port > 0) {
                config = config.port(Integer.toString(port));
                logger.info("Setting port to {}", port);
            }

            OmmProvider provider = EmaFactory.createOmmProvider(config, appClient);


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
        return dataTable.subscribeKeyedData(new DataForwarder(provider));
    }

    public void onReqMsg(ReqMsg reqMsg, OmmProviderEvent event) {
        logger.info("onReqMsg {}", reqMsg);

        switch (reqMsg.domainType()) {
            case EmaRdm.MMT_LOGIN:
                processLoginRequest(reqMsg, event);
                break;
            case EmaRdm.MMT_MARKET_PRICE:
                processMarketPriceRequest(reqMsg, event);
                break;
            case EmaRdm.MMT_DIRECTORY:
                directoryRequestProcessor.processRequest(reqMsg, event);
                break;
            case EmaRdm.MMT_DICTIONARY:
                processDictionaryRequest(reqMsg, event);
                break;
            case EmaRdm.MMT_SYMBOL_LIST:
                symbolListProcessor.processRequest(reqMsg, event);
                break;
            default:
                processInvalidItemRequest(reqMsg, event);
                break;
        }
    }

    public void onRefreshMsg(RefreshMsg refreshMsg, OmmProviderEvent event) {
        logger.info("onRefreshMsg {}", refreshMsg);
    }

    public void onStatusMsg(StatusMsg statusMsg, OmmProviderEvent event) {
        logger.info("onStatusMsg {}", statusMsg);
    }

    public void onGenericMsg(GenericMsg genericMsg, OmmProviderEvent event) {
        logger.info("onGenericMsg {}", genericMsg);
    }

    public void onPostMsg(PostMsg postMsg, OmmProviderEvent event) {
        logger.info("onPostMsg {}", postMsg);
    }

    public void onReissue(ReqMsg reqMsg, OmmProviderEvent event) {
        switch (reqMsg.domainType())
        {
            case EmaRdm.MMT_DICTIONARY:
                processDictionaryRequest(reqMsg, event);
                break;
            default:
                processInvalidItemRequest(reqMsg, event);
                break;
        }
    }

    public void onClose(ReqMsg reqMsg, OmmProviderEvent event) {
    }

    public void onAllMsg(Msg msg, OmmProviderEvent event) {
    }


    void processLoginRequest(ReqMsg reqMsg, OmmProviderEvent event) {
        logger.info("processLoginRequest for user {}", reqMsg.name());

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

    void processDictionaryRequest(ReqMsg reqMsg, OmmProviderEvent event) {
        Series series = EmaFactory.createSeries();

        boolean done = false;
        refreshMsg.clear().clearCache(true);

        int currentValue;

        if (reqMsg.name().equals("RWFFld")) {
            currentValue = dataDictionary.minFid();

            while (!done) {
                currentValue = dataDictionary.encodeFieldDictionary(
                        series, currentValue, reqMsg.filter(), FRAGMENTATION_SIZE);

                done = currentValue == dataDictionary.maxFid();

                event.provider().submit(refreshMsg.name(reqMsg.name())
                        .serviceName(reqMsg.serviceName())
                        .domainType(EmaRdm.MMT_DICTIONARY)
                        .filter(reqMsg.filter())
                        .payload(series)
                        .complete(done).
                        solicited(true), event.handle());

                refreshMsg.clear();
            }
        } else if (reqMsg.name().equals("RWFEnum")) {
            currentValue = 0;

            while (!done) {
                currentValue = dataDictionary.encodeEnumTypeDictionary(
                        series, currentValue, reqMsg.filter(), FRAGMENTATION_SIZE);

                done = currentValue == dataDictionary.enumTables().size();

                event.provider().submit(refreshMsg.name(reqMsg.name())
                        .serviceName(reqMsg.serviceName())
                        .domainType(EmaRdm.MMT_DICTIONARY)
                        .filter(reqMsg.filter())
                        .payload(series)
                        .complete(done).
                        solicited(true), event.handle());

                refreshMsg.clear();
            }
        }
    }


    void processMarketPriceRequest(ReqMsg reqMsg, OmmProviderEvent event) {

        String key = reqMsg.name();

        Set<Long> handleSet = handles.get(key);

        long handle = event.handle();
        if (handleSet != null && !handleSet.contains(handle)) {
            processInvalidItemRequest(reqMsg, event);
            return;
        } else {
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

                provider.submit(updateMsg, client);
            }
        }
    }


}
