package dido.vertx;

import org.oddjob.Oddjob;
import org.oddjob.state.ParentState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.*;
import java.security.cert.CertificateException;
import java.util.Objects;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class PkiStoresUtil {

    private static final Logger logger = LoggerFactory.getLogger(PkiStoresUtil.class);

    public enum Store {
        CLIENT_KEYS( "client_keystore.p12","clistorepwd"),
        CLIENT_TRUST("client_trustore.p12", "clitrustpwd"),
        SERVER_KEYS("server_keystore.p12", "srvstorepwd"),
        SERVER_TRUST("server_trustore.p12",  "srvtrustpwd"),;

        private final String storeName;

        private final String password;

        Store(String storeName, String password) {
            this.storeName = storeName;
            this.password = password;
        }
    }

    private final File workDir;



    private PkiStoresUtil(File workDir) {
        this.workDir = workDir;
    }

    public static PkiStoresUtil of(String workDir) {
        return of(new File(workDir));
    }

    public static PkiStoresUtil of(File workDir) {
        return new PkiStoresUtil(workDir);
    }

    public PkiStoresUtil createStoresIfMissing() {

        if (workDir.mkdirs()) {
            logger.info("Creating stores in dir {}", workDir.getAbsolutePath());
            return createStores();
        }
        else {
            logger.info("Stores already exist in dir {}", workDir.getAbsolutePath());
            return this;
        }
    }

    public Properties getProperties() {
        Properties properties = new Properties();
        properties.setProperty("work.dir", workDir.getAbsolutePath());
        return properties;
    }

    public PkiStoresUtil createStores() {

        File setupPkiFile = new File(Objects.requireNonNull(
                PkiStoresUtil.class.getResource("/examples/SetupPkiStores.xml")).getFile());

        Oddjob pkiOddjob = new Oddjob();
        pkiOddjob.setFile(setupPkiFile);
        pkiOddjob.setProperties(getProperties());

        pkiOddjob.run();

        assertThat(pkiOddjob.lastStateEvent().getState(), is(ParentState.COMPLETE));

        return this;
    }

    public Path storePath(Store store) {
        return workDir.toPath().resolve("stores").resolve(store.storeName);
    }

    public KeyStore loadStore(Store store) throws KeyStoreException, IOException, CertificateException, NoSuchAlgorithmException {

        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        try (InputStream ks = Files.newInputStream(storePath(store))) {
            keyStore.load(ks, store.password.toCharArray());
        }
        return keyStore;
    }

    public KeyManagerFactory loadKeyManagerFactory(Store store) throws CertificateException, KeyStoreException, IOException, NoSuchAlgorithmException, UnrecoverableKeyException {

        KeyStore keyStore = loadStore(store);
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keyStore, store.password.toCharArray());

        return kmf;
    }

    public TrustManagerFactory loadTrustManagerFactory(Store store) throws CertificateException, KeyStoreException, IOException, NoSuchAlgorithmException {

        KeyStore trustStore = loadStore(store);

        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(trustStore);

        return tmf;
    }

    public SSLContext createSSLContext(Store keyStoreDef, Store trustStoreDef) throws KeyStoreException, IOException, CertificateException, NoSuchAlgorithmException, UnrecoverableKeyException, KeyManagementException {

        KeyManagerFactory kmf = loadKeyManagerFactory(keyStoreDef);

        TrustManagerFactory tmf = null;

        if (trustStoreDef != null) {
            tmf = loadTrustManagerFactory(trustStoreDef);
        }

        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(kmf.getKeyManagers(),
                tmf == null ? null : tmf.getTrustManagers(),
                new SecureRandom());

        return sslContext;
    }

}
