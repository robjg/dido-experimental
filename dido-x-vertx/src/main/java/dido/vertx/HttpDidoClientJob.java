package dido.vertx;

import dido.data.DidoData;
import dido.how.DataInHow;
import io.vertx.core.Vertx;
import io.vertx.ext.auth.authentication.Credentials;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

/**
 * @oddjob.description An HTTP Client for reading Dido Data.
 *
 * @oddjob.example A simple client that consumes a REST endpoint as Dido Data.
 * {@oddjob.xml.resource examples/HttpDidoClientExample.xml}
 */
public class HttpDidoClientJob implements Callable<CompletableFuture<Integer>> {

    private static final Logger logger = LoggerFactory.getLogger(HttpDidoClientJob.class);

    private String name;

    private Vertx vertx;

    private URL url;

    private DataInHow<InputStream> dataInHow;

    private Consumer<? super DidoData> to;

    private ClientOptionsModifier sslOptions;

    private Credentials credentials;

    private Executor executor;

    private Runnable close;

    @Override
    public CompletableFuture<Integer> call() throws Exception {

        Executor executor = Objects.requireNonNull(this.executor, "No executor provided");

        DataInHow<InputStream> dataInHow = Objects.requireNonNull(this.dataInHow, "No data in how provided");

        List<Runnable> closes = new ArrayList<>();

        if (vertx == null) {
            vertx = Vertx.vertx();
            closes.add(() -> {
                vertx.close();
                vertx = null;
            });
        }

        WebClientOptions options = new WebClientOptions();

        Optional.ofNullable(this.sslOptions)
                .ifPresent(sslOptions -> sslOptions.modify(options, vertx));

        WebClient client = WebClient.create(vertx, options);

        CompletableFuture<Integer> future = new CompletableFuture<>();

        int port = url.getPort();
        String host = url.getHost();
        String path = url.getPath();

        client.get(port, host, path)
                .send()
                .onSuccess(response -> {
                    logger.info("Received response with status code {}", response.statusCode());
                    executor.execute(() -> {
                        ByteArrayInputStream stream = new ByteArrayInputStream(response.body().getBytes());

                        dataInHow.inFrom(stream).forEach(to);

                        future.complete(0);
                    });

                })
                .onFailure(err -> {
                    logger.error("Something went wrong ", err);
                    future.completeExceptionally(err);
                });

        closes.add(client::close);

        this.close = () -> closes.forEach(Runnable::run);

        return future.whenComplete((result, err) -> stop());
    }

    public void stop() {
        Optional.ofNullable(close)
                .ifPresent(cl ->
                {
                    cl.run();
                    close = null;
                });
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public URL getUrl() {

        return url;
    }

    public void setUrl(URL url) {
        this.url = url;
    }

    public DataInHow<InputStream> getDataInHow() {
        return dataInHow;
    }

    public void setDataInHow(DataInHow<InputStream> dataInHow) {
        this.dataInHow = dataInHow;
    }

    public Consumer<? super DidoData> getTo() {
        return to;
    }

    public void setTo(Consumer<? super DidoData> to) {
        this.to = to;
    }

    public ClientOptionsModifier getSslOptions() {
        return sslOptions;
    }

    public void setSslOptions(ClientOptionsModifier sslOptions) {
        this.sslOptions = sslOptions;
    }

    @Inject
    public Executor getExecutor() {
        return executor;
    }

    public void setExecutor(Executor executor) {
        this.executor = executor;
    }

    @Override
    public String toString() {
        return name == null ? getClass().getSimpleName() : name;
    }
}
