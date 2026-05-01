package dido.elsewhere.ema;

import com.refinitiv.ema.access.*;
import dido.data.DataSchema;
import dido.table.internal.DataTableBasic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class EmaConsumerService {

    private static final Logger logger = LoggerFactory.getLogger(EmaConsumerService.class);

    private String name;

    private String host;

    private String serviceName;

    private List<String> symbols;

    private DataTableBasic<String> table;

    private DataSchema schema;

    private Runnable close;

    class AppClient implements OmmConsumerClient {

        DidoOmmData didoOmmData;


        public void onRefreshMsg(RefreshMsg refreshMsg, OmmConsumerEvent event) {

            logger.debug("onRefreshMsg: {}", refreshMsg);

            if (DataType.DataTypes.FIELD_LIST == refreshMsg.payload().dataType()) {

                FieldList fieldList = refreshMsg.payload().fieldList();
                didoOmmData = DidoOmmData.of(fieldList);

                table.onData(didoOmmData.data(fieldList));
            }
            else {
                logger.warn("onRefreshMsg: unsupported data type");
            }


        }

        public void onUpdateMsg(UpdateMsg updateMsg, OmmConsumerEvent event) {

            logger.debug("onUpdateMsg: {}", updateMsg);

            if (DataType.DataTypes.FIELD_LIST == updateMsg.payload().dataType()) {

                FieldList fieldList = updateMsg.payload().fieldList();

                table.onPartial(didoOmmData.partial(fieldList));
            }
            else {
                logger.warn("onUpdateMsg: unsupported data type");
            }

            System.out.println(updateMsg);
        }

        public void onStatusMsg(StatusMsg statusMsg, OmmConsumerEvent event) {
            System.out.println(statusMsg);
        }

        public void onGenericMsg(GenericMsg genericMsg, OmmConsumerEvent consumerEvent) {
        }

        public void onAckMsg(AckMsg ackMsg, OmmConsumerEvent consumerEvent) {
        }

        public void onAllMsg(Msg msg, OmmConsumerEvent consumerEvent) {
        }
    }

    public void start() {

        logger.info("Starting EmaConsumerService for host: {} and symbols: {}",
                host, symbols);

        this.table = DataTableBasic.<String>withSchema(schema)
                .create();

        OmmConsumerConfig config = EmaFactory.createOmmConsumerConfig();

        OmmConsumer consumer = EmaFactory.createOmmConsumer(
                config.host(host)
                        .username("user"));

        for (String symbol : symbols) {

            AppClient appClient = new AppClient();

            ReqMsg reqMsg = EmaFactory.createReqMsg();

            consumer.registerClient(reqMsg.serviceName(serviceName)
                    .name(symbol), appClient);
        }

        close = () -> {
            consumer.uninitialize();
            table = null;
        };
    }

    public void stop() {
        close.run();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public List<String> getSymbols() {
        return symbols;
    }

    public void setSymbols(List<String> symbols) {
        this.symbols = symbols;
    }

    public DataSchema getSchema() {
        return schema;
    }

    public void setSchema(DataSchema schema) {
        this.schema = schema;
    }

    public DataTableBasic<String> getTable() {
        return table;
    }

    @Override
    public String toString() {
        return name == null ? getClass().getSimpleName() : name;
    }
}
