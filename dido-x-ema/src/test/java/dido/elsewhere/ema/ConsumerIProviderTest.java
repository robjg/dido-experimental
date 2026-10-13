package dido.elsewhere.ema;

import dido.data.DidoData;
import dido.data.partial.PartialData;
import dido.flow.KeyedDataConsumer;
import dido.flow.QuietlyCloseable;
import dido.table.DataTable;
import org.junit.jupiter.api.Test;
import org.oddjob.Oddjob;
import org.oddjob.OddjobLookup;
import org.oddjob.arooa.convert.ArooaConversionException;
import org.oddjob.arooa.utils.TypeToken;
import org.oddjob.state.StateConditions;
import org.oddjob.tools.StateSteps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class ConsumerIProviderTest {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerIProviderTest.class);

    @Test
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
                .lookup("consumer.table",
                        new TypeToken<DataTable<String>>() {}.getType());

        // Wait for a few table updates
        CountDownLatch latch = new CountDownLatch(3);

        logger.info("** Subscribing to Ticks");

        try (QuietlyCloseable ignored = clientTable.subscribe(new KeyedDataConsumer<>() {
            @Override
            public void onData(String key, DidoData data) {
            }

            @Override
            public void onPartial(String key, PartialData partial) {
                logger.info("** On Partial {}", partial);
                latch.countDown();
            }

            @Override
            public void onDelete(String key) {

            }
        })) {

            if (!latch.await(10, TimeUnit.SECONDS)) {
                assertThat("Ticker Missing", false);
            }

            assertThat(clientTable.size(), is(3));
        }

        clientOddjob.destroy();
        providerOddjob.destroy();

    }
}
