package dido.elsewhere.ema;

import dido.data.DidoData;
import dido.data.partial.PartialUpdate;
import dido.table.DataTable;
import dido.table.KeyedSubscriber;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.oddjob.Oddjob;
import org.oddjob.OddjobLookup;
import org.oddjob.arooa.convert.ArooaConversionException;
import org.oddjob.state.StateConditions;
import org.oddjob.tools.StateSteps;

import java.io.File;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;

class ConsumerIProviderTest {

    @Test
    @Disabled
    void iProviderConsumer() throws InterruptedException, ArooaConversionException {

        File providerConfig = new File(Objects.requireNonNull(
                getClass().getResource("/examples/EmaIProvider.xml")).getFile());
        File clientConfig = new File(Objects.requireNonNull(
                getClass().getResource("/examples/EmaConsumer.xml")).getFile());

        Oddjob providerOddjob = new Oddjob();
        providerOddjob.setFile(providerConfig);

        StateSteps serverStates = new StateSteps(providerOddjob);
        serverStates.startCheck(StateConditions.READY,
                StateConditions.EXECUTING, StateConditions.STARTED);

        providerOddjob.run();

        serverStates.checkWait();

        Oddjob clientOddjob = new Oddjob();
        clientOddjob.setFile(clientConfig);

        StateSteps clientStates = new StateSteps(clientOddjob);
        clientStates.startCheck(StateConditions.READY,
                StateConditions.EXECUTING, StateConditions.STARTED);

        clientOddjob.run();

        clientStates.checkWait();

        DataTable<String> clientTable = new OddjobLookup(clientOddjob)
                .lookup("consumer.table", DataTable.class);


        CountDownLatch latch = new CountDownLatch(3);

        clientTable.tableSubscribe(new KeyedSubscriber<String>() {
            @Override
            public void onData(String key, DidoData data) {
                latch.countDown();
            }

            @Override
            public void onPartial(String key, PartialUpdate data) {

            }

            @Override
            public void onDelete(String key) {

            }
        });

        if (!latch.await(5, TimeUnit.SECONDS)) {
            assertThat("Ticker Missing", false);
        }

        clientOddjob.destroy();
        providerOddjob.destroy();

    }

}
