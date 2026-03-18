package dido.vertx;

import io.vertx.core.Vertx;

public class VertxService {

    private String name;

    private Vertx vertx;

    public void start() {

        vertx = Vertx.vertx();
    }

    public void stop() {

        if (vertx != null) {
            vertx.close();
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

    @Override
    public String toString() {
        return name == null ? getClass().getSimpleName() : name;
    }
}
