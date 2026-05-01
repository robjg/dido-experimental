package dido.elsewhere.ema;

import dido.data.DataSchema;
import dido.data.DidoData;
import dido.data.partial.PartialUpdateIndexed;
import dido.table.DataTable;
import dido.table.internal.DataTableBasic;
import org.oddjob.framework.Service;

import javax.inject.Inject;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class TickingTableService implements Service {

    public static final DataSchema SCHEMA = DataSchema.builder()
            .addNamed("SYMBOL", String.class)
            .addNamed("BID", double.class)
            .addNamed("ASK", double.class)
            .addNamed("BIDSIZE", int.class)
            .addNamed("ASKSIZE", int.class)
            .build();

    private ScheduledExecutorService executorService;

    private DataTable<String> table;

    private Runnable close;

    @Override
    public void start() throws Exception {

        DataTableBasic<String> tableBasic =DataTableBasic.<String>withSchema(SCHEMA)
                .create();

        DidoData.withSchema(SCHEMA)
                .many()
                .of("IBM.N", 99.9, 100.1, 80, 90)
                .of("APPL.OQ", 104.9, 105.1, 30, 20)
                .of("MSFT.OQ", 79.9, 80.1, 110, 100)
                .toStream().forEach(tableBasic::onData);

        Wander ibmBid = new Wander(99.9);

        ScheduledFuture<?> future = executorService.scheduleAtFixedRate(() -> {
            tableBasic.onPartial(PartialUpdateIndexed.of(
                    DidoData.of("IBM.N", ibmBid.next(), 100.1, 80, 90), 2));
        }, 3, 3, TimeUnit.SECONDS);

        table = tableBasic;

        close = () -> {
            future.cancel(false);
            table = null;
        };
    }

    @Override
    public void stop() {

        close.run();
    }

    static class Wander {

        private double value;

        private int count;

        Wander(double value) {
            this.value = value;
        }

        double next() {
            if (count++/10%2 == 0) {
                value += 0.1;
            }
            else  {
                value -= 0.1;
            }
            return value;
        }
    }

    public ScheduledExecutorService getExecutorService() {
        return executorService;
    }

    @Inject
    public void setExecutorService(ScheduledExecutorService executorService) {
        this.executorService = executorService;
    }

    public DataTable<String> getTable() {
        return table;
    }

    @Override
    public String toString() {
        return "TickingTableService";
    }
}
