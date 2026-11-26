package dido.elsewhere.ema;

import com.refinitiv.ema.access.EmaFactory;
import com.refinitiv.ema.access.OmmConsumer;
import com.refinitiv.ema.access.OmmConsumerConfig;
import com.refinitiv.ema.access.ReqMsg;
import com.refinitiv.ema.rdm.DataDictionary;
import dido.flow.QuietlyCloseable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Wraps a {@link OmmConsumer} so that it may accept {@link DidoOmmConsumerClient}s.
 */
public class DidoOmmConsumerConnection
        implements DidoOmmConsumer, QuietlyCloseable {

    private static final Logger logger = LoggerFactory.getLogger(DidoOmmConsumerConnection.class);

    private final OmmConsumer consumer;

    private final AtomicReference<DataDictionary> dictionaryRef;

    /**
     * Currently using the field list, if no dictionary.
     */
    private final List<AutoCloseable> closeTasks;

    DidoOmmConsumerConnection(OmmConsumer consumer,
                              AtomicReference<DataDictionary> dictionaryRef,
                              List<AutoCloseable> closeTasks) {
        this.consumer = consumer;
        this.dictionaryRef = dictionaryRef;
        this.closeTasks = closeTasks;
    }

    @Override
    public QuietlyCloseable register(DidoOmmConsumerClient client) {
        DataDictionary dictionary = Objects.requireNonNull(dictionaryRef.get(),
                "Dictionary not available yet.");

        return client.start(dictionary, consumer);
    }

    @Override
    public void close() {
        closeTasks.forEach(closeTask -> {
            try {
                closeTask.close();
            } catch (Exception e) {
                logger.warn("Failed closing {}", closeTask, e);
            }
        });
        consumer.uninitialize();
    }

    public static class Settings {

        private String host;

        private String serviceName;

        private Path configPath;

        private Path dictionaryDir;

        private String username;

        private String password;

        private Runnable started;

        public Settings host(String host) {
            this.host = host;
            return this;
        }

        public Settings serviceName(String serviceName) {
            this.serviceName = serviceName;
            return this;
        }

        public Settings configPath(Path configPath) {
            this.configPath = configPath;
            return this;
        }

        public Settings dictionaryDir(Path dictionaryDir) {
            this.dictionaryDir = dictionaryDir;
            return this;
        }

        public Settings username(String username) {
            this.username = username;
            return this;
        }

        public Settings password(String password) {
            this.password = password;
            return this;
        }

        public Settings started(Runnable started) {
            this.started = started;
            return this;
        }

        public DidoOmmConsumerConnection start() {

            List<AutoCloseable> closeTasks = new ArrayList<>();

            String serviceName = Objects.requireNonNullElse(this.serviceName,
                    "DIRECT_FEED");

            OmmConsumerConfig config;
            if (configPath == null) {
                logger.info("Creating default OmmConsumerConfig");
                config = EmaFactory.createOmmConsumerConfig();
            } else {
                logger.info("Creating OmmConsumerConfig from {}", configPath);
                config = EmaFactory.createOmmConsumerConfig(configPath.toString());
            }

            if (host != null) {
                config = config.host(host);
            }
            if (username != null) {
                config = config.username(username);
            }
            if (password != null) {
                config = config.password(password);
            }

            OmmConsumer consumer = EmaFactory.createOmmConsumer(config);

            List<Consumer<? super DataDictionary>> dictionaryConsumers = new ArrayList<>();

            AtomicReference<DataDictionary> dictionaryRef = new AtomicReference<>();
            dictionaryConsumers.add(dictionaryRef::set);

            DidoOmmConsumerConnection client = new DidoOmmConsumerConnection(
                    consumer, dictionaryRef, closeTasks);

            ReqMsg reqMsg = EmaFactory.createReqMsg();

            if (started != null) {
                dictionaryConsumers.add(ignored -> {
                    logger.info("Signalling Consumer Start.");
                    started.run();
                });
            }

            DataDictionary dictionary = EmaFactory.createDataDictionary();
            Consumer<DataDictionary> onDictionary = d -> dictionaryConsumers.forEach(
                    c -> c.accept(d));
            if (dictionaryDir == null) {
                logger.info("No local dictionary. Attempting to download dictionary.");
                DictionaryConsumerClient dictionaryClient = new DictionaryConsumerClient(
                        dictionary, reqMsg, onDictionary);
                dictionaryClient.register(consumer);
            } else {
                dictionary.loadFieldDictionary(dictionaryDir
                        .resolve("RDMFieldDictionary").toString());
                dictionary.loadEnumTypeDictionary(dictionaryDir
                        .resolve("enumtype.def").toString());
                onDictionary.accept(dictionary);
            }

            logger.info("Started Consumer to host {}, service name {}", host, serviceName);

            return client;
        }
    }

    public static Settings with() {
        return new Settings();
    }
}
