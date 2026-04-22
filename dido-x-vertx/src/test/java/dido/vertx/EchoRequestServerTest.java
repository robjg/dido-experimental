package dido.vertx;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.oddjob.Oddjob;
import org.oddjob.OddjobLookup;
import org.oddjob.arooa.convert.ArooaConversionException;
import org.oddjob.state.ParentState;
import org.oddjob.tools.StateSteps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Objects;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;

class EchoRequestServerTest {

    private static final Logger logger = LoggerFactory.getLogger(EchoRequestServerTest.class);

    PkiStoresUtil storeUtil;

    @BeforeEach
    void setUp() {
        storeUtil = PkiStoresUtil.of( "target/work/SslConfigurationTest")
                .createStoresIfMissing();
    }


    @Test
    void roundTrip() throws InterruptedException, ArooaConversionException, UnrecoverableKeyException, CertificateException, KeyStoreException, IOException, NoSuchAlgorithmException, KeyManagementException {

        File serverFile = new File(Objects.requireNonNull(
                getClass().getResource("/examples/EchoRequestServer.xml")).getFile());

        Properties serverProps = new Properties();
        serverProps.putAll(storeUtil.getProperties());
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

        String hostname = java.net.InetAddress.getLocalHost().getHostName();

        URI uri = URI.create("https://" + hostname + ":" + serverPort + "/foo");

        try (HttpClient client = HttpClient.newBuilder()
                .sslContext(storeUtil.createSSLContext(
                        PkiStoresUtil.Store.CLIENT_KEYS,
                        PkiStoresUtil.Store.CLIENT_TRUST))
                .build()) {

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(uri)
                    .GET()
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

            System.out.println(response.statusCode());
            System.out.println(response.body());
        }

        serverOddjob.destroy();
    }

}