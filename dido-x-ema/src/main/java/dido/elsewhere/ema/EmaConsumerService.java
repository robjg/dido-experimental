package dido.elsewhere.ema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;

/**
 * @oddjob.description A server that provides a Consumer of LSEG Real Time Data.
 * T
 */
public class EmaConsumerService {

    private static final Logger logger = LoggerFactory.getLogger(EmaConsumerService.class);

    /**
     * @oddjob.description The name of the service as seen in Oddjob.
     * @oddjob.required No.
     */
    private String name;

    /**
     * @oddjob.description The path to a config file.
     * @oddjob.required No. See LSEG docs for defaults.
     */
    private Path configPath;

    /**
     * @oddjob.description The host address in server:port format.
     * @oddjob.required No. See LSEG docs for defaults.
     */
    private String host;

    /**
     * @oddjob.description The username if required.
     * @oddjob.required No.
     */
    private String username;

    /**
     * @oddjob.description The password if required.
     * @oddjob.required No.
     */
    private String password;

    /**
     * @oddjob.description The directory where dictionary files are.
     * At the moment dictionary files must exist locally.
     * @oddjob.required No. Defaults to "."
     */
    private Path dictionaryDir;

    /**
     * @oddjob.description The consumer created by this service.
     * @oddjob.required Read only.
     */
    private DidoOmmConsumerConnection consumer;

    public CompletableFuture<Void> start() {

        logger.info("Starting EmaConsumerService for host: {}", host);

        CompletableFuture<Void> future = new CompletableFuture<>();

        this.consumer = DidoOmmConsumerConnection.with()
                .configPath(configPath)
                .host(host)
                .username(username)
                .password(password)
                .dictionaryDir(dictionaryDir)
                .started(() -> future.complete(null))
                .start();

        return future;
    }

    public void stop() {
        consumer.close();
    }

    public DidoOmmConsumer getConsumer() {
        return consumer;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Path getConfigPath() {
        return configPath;
    }

    public void setConfigPath(Path configPath) {
        this.configPath = configPath;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public Path getDictionaryDir() {
        return dictionaryDir;
    }

    public void setDictionaryDir(Path dictionaryDir) {
        this.dictionaryDir = dictionaryDir;
    }

    @Override
    public String toString() {
        return name == null ? getClass().getSimpleName() : name;
    }
}
