package dido.vertx;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerOptions;

public interface ServerOptionsModifier {

    void modify(HttpServerOptions serverOptions, Vertx vertx);
}
