package dido.vertx;

import org.junit.jupiter.api.Test;
import org.oddjob.Oddjob;
import org.oddjob.OddjobLookup;
import org.oddjob.arooa.convert.ArooaConversionException;
import org.oddjob.state.ParentState;
import org.oddjob.tools.StateSteps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

class HttpDidoClientServerTest {

    private static final Logger logger = LoggerFactory.getLogger(HttpDidoClientServerTest.class);

    @Test
    void roundTrip() throws InterruptedException, ArooaConversionException {

        File serverFile = new File(Objects.requireNonNull(
                getClass().getResource("/examples/HttpDidoServerExample.xml")).getFile());
        File clientFile = new File(Objects.requireNonNull(
                getClass().getResource("/examples/HttpDidoClientExample.xml")).getFile());

        Properties serverProps = new Properties();
        serverProps.setProperty("example.server.port", "0");

        Oddjob serverOddjob = new Oddjob();
        serverOddjob.setFile(serverFile);
        serverOddjob.setProperties(serverProps);

        StateSteps serverStates = new StateSteps(serverOddjob);

        serverStates.startCheck(
                StateSteps.definitely(ParentState.READY),
                StateSteps.definitely(ParentState.EXECUTING),
                StateSteps.maybe(ParentState.ACTIVE),
                StateSteps.definitely(ParentState.STARTED));

        serverOddjob.run();

        serverStates.checkWait();

        String serverPort = new OddjobLookup(serverOddjob)
                .lookup("server.port", String.class);

        logger.info("Server port: {}", serverPort);

        assertThat(serverPort, notNullValue());

        Properties clientProps = new Properties();
        clientProps.setProperty("example.server.port", serverPort);

        Oddjob clientOddjob = new Oddjob();
        clientOddjob.setFile(clientFile);
        clientOddjob.setProperties(clientProps);

        StateSteps clientStates = new StateSteps(clientOddjob);

        clientStates.startCheck(
                StateSteps.definitely(ParentState.READY),
                StateSteps.definitely(ParentState.EXECUTING),
                StateSteps.maybe(ParentState.ACTIVE),
                StateSteps.maybe(ParentState.STARTED),
                StateSteps.definitely(ParentState.COMPLETE));

        clientOddjob.run();

        clientStates.checkWait();

        List<?> actual = new OddjobLookup(clientOddjob).lookup("data.list", List.class);
        List<?> expected = new OddjobLookup(serverOddjob).lookup("dido/data.list", List.class);

        assertThat(actual, is(expected));

        clientOddjob.destroy();
        serverOddjob.destroy();
    }
}