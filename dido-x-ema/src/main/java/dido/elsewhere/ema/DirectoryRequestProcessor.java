package dido.elsewhere.ema;

import com.refinitiv.ema.access.*;
import com.refinitiv.ema.domain.directory.DirectoryRefresh;
import com.refinitiv.ema.domain.directory.DirectoryService;
import com.refinitiv.ema.domain.directory.DirectoryServiceInfo;
import com.refinitiv.ema.domain.directory.DirectoryServiceState;
import com.refinitiv.ema.rdm.EmaRdm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class DirectoryRequestProcessor {

    private static final Logger logger = LoggerFactory.getLogger(DirectoryRequestProcessor.class);

    private final DirectoryRefresh directoryRefresh =  EmaFactory.Domain.createDirectoryRefresh();
    private final DirectoryService directoryService = EmaFactory.Domain.createDirectoryService();
    private final DirectoryServiceState directoryServiceState = EmaFactory.Domain.createDirectoryServiceState();
    private final DirectoryServiceInfo directoryServiceInfo = EmaFactory.Domain.createDirectoryServiceInfo();

    private final String serviceName;

    public DirectoryRequestProcessor(String serviceName) {
        this.serviceName = serviceName;
    }

    public void processRequest(ReqMsg reqMsg, OmmProviderEvent event) {

        logger.info("Processing Directory Request as service {}", serviceName);

        directoryRefresh.clear();
        directoryService.clear();
        directoryServiceState.clear();
        directoryServiceInfo.clear();

        directoryServiceInfo.serviceName(serviceName);
        directoryServiceInfo.capabilitiesList(Arrays.asList(
                (long)EmaRdm.MMT_DICTIONARY,
                (long)EmaRdm.MMT_MARKET_PRICE,
                (long)EmaRdm.MMT_MARKET_BY_PRICE));
        directoryServiceInfo.dictionariesUsedList(Arrays.asList("RWFFld", "RWFEnum"));
        directoryServiceInfo.dictionariesProvidedList(Arrays.asList("RWFFld", "RWFEnum"));
        directoryServiceInfo.action(FilterEntry.FilterAction.SET);

        directoryServiceState.serviceState(EmaRdm.SERVICE_UP);
        directoryServiceState.action(FilterEntry.FilterAction.SET);

        directoryService.serviceId(2);
        directoryService.action(MapEntry.MapAction.ADD);
        directoryService.state(directoryServiceState);
        directoryService.info(directoryServiceInfo);

        directoryRefresh.serviceList().add(directoryService);
        directoryRefresh.filter(EmaRdm.SERVICE_INFO_FILTER | EmaRdm.SERVICE_STATE_FILTER);
        directoryRefresh.complete(true);
        directoryRefresh.solicited(true);
        directoryRefresh.clearCache(true);

        event.provider().submit(directoryRefresh.message(), event.handle());
    }


}
