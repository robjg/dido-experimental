package dido.vertx;

import io.vertx.core.Vertx;
import io.vertx.ext.web.client.WebClientOptions;

public interface ClientOptionsModifier {

    void modify(WebClientOptions clientOptions, Vertx vertx);
}
