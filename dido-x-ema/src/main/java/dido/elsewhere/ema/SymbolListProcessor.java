package dido.elsewhere.ema;

import com.refinitiv.ema.access.*;
import com.refinitiv.ema.rdm.EmaRdm;
import dido.table.DataTable;

/**
 * <a href="https://github.com/LSEG-API-Samples/Article.EMA.JavaCpp.SymbolListNIP/blob/master/EMAJ/com/refinitiv/ema/examples/training/niprovider/example__SymbolList__Streaming/NiProvider.java"></a>
 */
public class SymbolListProcessor {

    private final String name;

    private final String serviceName;

    private final DataTable<String> dataTable;

    private final RefreshMsg refreshMsg;

    private final com.refinitiv.ema.access.Map symbolListMap = EmaFactory.createMap();


    public SymbolListProcessor(String name,
                               String serviceName,
                               DataTable<String> dataTable,
                               RefreshMsg refreshMsg) {
        this.name = name;
        this.serviceName = serviceName;
        this.dataTable = dataTable;
        this.refreshMsg = refreshMsg;
    }

    public void processRequest(ReqMsg reqMsg, OmmProviderEvent event) {

        for (String key : dataTable.keySet()) {

            symbolListMap.add(EmaFactory.createMapEntry()
                    .keyAscii(key, MapEntry.MapAction.ADD));
        }

        refreshMsg.domainType(EmaRdm.MMT_SYMBOL_LIST)
                .name(name) // The name of your symbol list item
                .serviceName(serviceName) // Your configured service name
                .state(OmmState.StreamState.OPEN, OmmState.DataState.OK, OmmState.StatusCode.NONE, "List Complete")
                .solicited(true) // Set to false if using a Non-Interactive Provider (NIP)
                .complete(true)
                .payload(symbolListMap);

        event.provider().submit(refreshMsg, event.handle());
    }

}
