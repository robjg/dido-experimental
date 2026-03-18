package dido.vertx;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.Router;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HttpServerService {

    private String name;

    private final Map<String, DidoOutEndpoint> endpoints = new HashMap<>();

    private Vertx vertx;

    private volatile Runnable close;

    public void start() {

        List<Runnable> closes = new ArrayList<>();

        if (vertx == null) {
            vertx = Vertx.vertx();
            closes.add(vertx::close);
        }

        Router router = Router.router(vertx);

        for (Map.Entry<String, DidoOutEndpoint> entry : endpoints.entrySet()) {
            String path =  entry.getKey();
            DidoOutEndpoint endpoint = entry.getValue();
            String mediaType = endpoint.getMediaType() == null ? "application/octet-stream" :  endpoint.getMediaType();

            router.route(path)
                    .respond(routingContext ->
                            routingContext.response()
                            .putHeader("content-type", mediaType)
                            .end(endpoint.get()));

        }

        HttpServer httpServer = vertx.createHttpServer();

        httpServer.requestHandler(router);

        closes.add(httpServer::close);

        this.close = () -> closes.forEach(Runnable::run);
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

    public void setEndpoints(String path, DidoOutEndpoint endpoint) {
        if (endpoint == null) {
            endpoints.remove(path);
        }
        else {
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
