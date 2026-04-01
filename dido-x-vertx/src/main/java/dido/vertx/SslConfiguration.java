package dido.vertx;

import io.vertx.core.Vertx;
import io.vertx.core.http.ClientAuth;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.net.KeyStoreOptions;
import io.vertx.core.net.SSLOptions;
import io.vertx.ext.web.client.WebClientOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HostnameVerifier;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.Objects;
import java.util.Optional;

/**
 * @oddjob.description Provides TLS Configuration for Client and Servers.
 */
public class SslConfiguration implements ClientOptionsModifier, ServerOptionsModifier {

    private static final Logger logger = LoggerFactory.getLogger(SslConfiguration.class);

    /**
     * @oddjob.property
     * @oddjob.description The path of the store that contains the private key and signed cert.
     * @oddjob.required Yes for Server, No for client unless doing Client Auth.
     */
    private volatile Path keyStorePath;

    /**
     * @oddjob.property
     * @oddjob.description The key store password.
     * @oddjob.required Yes.
     */
    private volatile String keyStorePassword;

    /**
     * @oddjob.property
     * @oddjob.description The key store type. Either JKS or PKCS12.
     * @oddjob.required No, defaults depending on JDK version.
     */
    private volatile String keyStoreType;

    /**
     * @oddjob.property
     * @oddjob.description The key password. Only applicable to JKS stores.
     * @oddjob.required No.
     */
    private volatile String keyManagerPassword;

    /**
     * @oddjob.property
     * @oddjob.description The path of the store that contains trusted public certs.
     * @oddjob.required No, unless you wish to verify your peer.
     */
    private volatile Path trustStorePath;

    /**
     * @oddjob.property
     * @oddjob.description The trust store password.
     * @oddjob.required Yes, if you have a trust store.
     */
    private volatile String trustStorePassword;

    /**
     * @oddjob.property
     * @oddjob.description The trust store type. Either JKS or PKCS12.
     * @oddjob.required No.
     */
    private volatile String trustStoreType;


    // Server Only

    /**
     * @oddjob.property
     * @oddjob.description Should the server perform client authentication. NONE/WANT/NEED.
     * @oddjob.required No, defaults to NONE, and only applicable to a server.
     */
    private volatile ClientAuth clientAuth;

    /**
     * @oddjob.property
     * @oddjob.description The port of the server ssl connector.
     * @oddjob.required No, can be set from the Server configuration.
     */
    private int port;

    // Client only

    /**
     * @oddjob.property
     * @oddjob.description Something that can verify if a hostname is acceptable when the host doesn't match
     * the certificate CN. In Jetty, to get this work, Client Endpoint Identification Algorithm is set to null. This
     * generates this warning: <em>No Client EndPointIdentificationAlgorithm configured for Client</em>
     * @oddjob.required No, and only applicable to a client.
     */
    private volatile HostnameVerifier hostnameVerifier;

    /**
     * @oddjob.property
     * @oddjob.description Should the client trust all certificates.
     * @oddjob.required No, defaults to false, and only applicable to a client.
     */
    private volatile boolean trustAll;

    /**
     * @oddjob.property
     * @oddjob.description Enable SNI (Server Name Indication).
     * @oddjob.required No, defaults to false.
     *
     */
    private volatile boolean sni;


    @Override
    public void modify(HttpServerOptions serverOptions, Vertx vertx) {

        serverOptions.setSsl(true)
                .setClientAuth(this.clientAuth)
                .setSni(sni);

        configureSslOptions(serverOptions.getSslOptions());

        if (logger.isDebugEnabled()) {
            debugServerOptions(serverOptions, vertx);
        }
    }

    @Override
    public void modify(WebClientOptions clientOptions, Vertx vertx) {

        clientOptions.setSsl(true)
                .setTrustAll(this.trustAll)
                .setForceSni(sni);

        configureSslOptions(clientOptions.getSslOptions());

        if (logger.isDebugEnabled()) {
            debugClientOptions(clientOptions, vertx);
        }
    }

    protected void configureSslOptions(SSLOptions sslOptions) {

        Optional.ofNullable(this.keyStorePath).map(Object::toString)
                .ifPresent(path -> {
                    KeyStoreOptions keyStoreOptions = new KeyStoreOptions()
                            .setPath(path);
                    Optional.ofNullable(this.keyStorePassword)
                            .ifPresent(keyStoreOptions::setPassword);
                    Optional.ofNullable(this.keyStoreType)
                            .ifPresent(keyStoreOptions::setType);
                    sslOptions.setKeyCertOptions(keyStoreOptions);
                });

        Optional.ofNullable(this.trustStorePath).map(Objects::toString)
                .ifPresent(path -> {
                    KeyStoreOptions keyStoreOptions = new KeyStoreOptions()
                            .setPath(path);
                    Optional.ofNullable(this.trustStorePassword)
                            .ifPresent(keyStoreOptions::setPassword);
                    Optional.ofNullable(this.trustStoreType)
                            .ifPresent(keyStoreOptions::setType);
                    sslOptions.setTrustOptions(keyStoreOptions);
                });
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public Path getKeyStorePath() {
        return keyStorePath;
    }

    public void setKeyStorePath(Path keyStorePath) {
        this.keyStorePath = keyStorePath;
    }

    public String getKeyStorePassword() {
        return keyStorePassword;
    }

    public void setKeyStorePassword(String keyStorePassword) {
        this.keyStorePassword = keyStorePassword;
    }

    public String getKeyStoreType() {
        return keyStoreType;
    }

    public void setKeyStoreType(String keyStoreType) {
        this.keyStoreType = keyStoreType;
    }

    public String getKeyManagerPassword() {
        return keyManagerPassword;
    }

    public void setKeyManagerPassword(String keyManagerPassword) {
        this.keyManagerPassword = keyManagerPassword;
    }

    public Path getTrustStorePath() {
        return trustStorePath;
    }

    public void setTrustStorePath(Path trustStorePath) {
        this.trustStorePath = trustStorePath;
    }

    public String getTrustStorePassword() {
        return trustStorePassword;
    }

    public void setTrustStorePassword(String trustStorePassword) {
        this.trustStorePassword = trustStorePassword;
    }

    public String getTrustStoreType() {
        return trustStoreType;
    }

    public void setTrustStoreType(String trustStoreType) {
        this.trustStoreType = trustStoreType;
    }

    public ClientAuth getClientAuth() {
        return clientAuth;
    }

    public void setClientAuth(ClientAuth clientAuth) {
        this.clientAuth = clientAuth;
    }

    public boolean isTrustAll() {
        return trustAll;
    }

    public void setTrustAll(boolean trustAll) {
        this.trustAll = trustAll;
    }

    public HostnameVerifier getHostnameVerifier() {
        return hostnameVerifier;
    }

    public void setHostnameVerifier(HostnameVerifier hostnameVerifier) {
        this.hostnameVerifier = hostnameVerifier;
    }

    public boolean isSni() {
        return sni;
    }

    public void setSni(boolean sni) {
        this.sni = sni;
    }

    @Override
    public String toString() {
        return "SslConfiguration{" +
                "keyStorePath=" + keyStorePath +
                ", keyStorePassword='" + keyStorePassword + '\'' +
                ", keyManagerPassword='" + keyManagerPassword + '\'' +
                ", trustStorePath=" + trustStorePath +
                ", trustStorePassword='" + trustStorePassword + '\'' +
                '}';
    }

    public static void debugServerOptions(HttpServerOptions serverOptions, Vertx vertx) {

        debug(serverOptions.getSslOptions(), vertx);
    }

    public static void debugClientOptions(WebClientOptions clientOptions, Vertx vertx) {

        debug(clientOptions.getSslOptions(), vertx);
    }

    public static void debug(SSLOptions sslOptions, Vertx vertx) {

        debug("Key Store",
                (KeyStoreOptions) sslOptions.getKeyCertOptions(), vertx);
        debug("Trust Store",
                (KeyStoreOptions) sslOptions.getTrustOptions(), vertx);
    }

    public static void debug(String storeName, KeyStoreOptions keyStoreOptions, Vertx vertx) {

        if (keyStoreOptions == null) {
            return;
        }

        logger.debug("{} contents is:", storeName);

        try {
            KeyStore keyStore =
                    keyStoreOptions.loadKeyStore(vertx);

            keyStore.aliases().asIterator()
                    .forEachRemaining(alias -> {
                        logger.debug("alias: {}", alias);
                    });

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}