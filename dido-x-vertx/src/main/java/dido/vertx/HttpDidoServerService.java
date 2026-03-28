package dido.vertx;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.Router;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * @oddjob.description An HTTP Server for publishing {@link DidoOutEndpoint}s.
 */
public class HttpDidoServerService {

    private static final Logger logger = LoggerFactory.getLogger(HttpDidoServerService.class);

    private String name;

    private final Map<String, DidoOutEndpoint> endpoints = new HashMap<>();

    private Vertx vertx;

    private volatile Runnable close;

    private int port;

    public CompletableFuture<Integer> start() {

        List<Runnable> closes = new ArrayList<>();

        if (vertx == null) {
            vertx = Vertx.vertx();
            closes.add(() -> {
                vertx.close();
                vertx = null;
            });
        }

        Router router = Router.router(vertx);

        for (Map.Entry<String, DidoOutEndpoint> entry : endpoints.entrySet()) {
            String path = entry.getKey();
            DidoOutEndpoint endpoint = entry.getValue();
            String mediaType = endpoint.getMediaType() == null ? "application/octet-stream" : endpoint.getMediaType();

            logger.info("Adding endpoint at {} for media type {}", path, mediaType);

            router.route(path)
                    .respond(routingContext ->
                            routingContext.response()
                                    .putHeader("content-type", mediaType)
                                    .end(endpoint.get()));
        }

        HttpServer httpServer = vertx.createHttpServer()
                .requestHandler(router);

        CompletableFuture<Integer> future = new CompletableFuture<>();

        httpServer.listen(this.port)
                .onComplete(server -> {
                    this.port = httpServer.actualPort();
                    logger.info("Http Server stated on port {}", this.port);
                    future.complete(0);
                })
                .onFailure(err -> {
                            logger.error("Failed to start", err);
                            future.completeExceptionally(err);
                        }
                );

        closes.add(() -> {
            httpServer.close();
            logger.info("Http Server closed.");
        });

        this.close = () -> closes.forEach(Runnable::run);

        return future;
    }

    public void stop() {

        if (close != null) {
            close.run();
            close = null;
        }
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Vertx getVertx() {
        return vertx;
    }

    public void setVertx(Vertx vertx) {
        this.vertx = vertx;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setEndpoints(String path, DidoOutEndpoint endpoint) {
        if (endpoint == null) {
            endpoints.remove(path);
        } else {
            endpoints.put(path, endpoint);
        }
    }

    public DidoOutEndpoint getEndpoints(String path) {
        return endpoints.get(path);
    }

    @Override
    public String toString() {
        return name == null ? getClass().getSimpleName() : name;
    }
}
