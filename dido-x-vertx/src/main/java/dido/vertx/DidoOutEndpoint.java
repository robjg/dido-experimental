package dido.vertx;

import dido.data.DidoData;
import dido.how.DataOut;
import dido.how.DataOutHow;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.RoutingContext;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * @oddjob.description Provides Dido Data to Vertx. Any type of Dido format that can
 * provide an {@code OutputStream} can be used.
 */
public class DidoOutEndpoint implements Supplier<Function<RoutingContext, Future<?>>> {

    private static final int DEFAULT_BUFFER_SIZE = 256;

    private Iterable<DidoData> data;

    private DataOutHow<OutputStream> how;

    private String mediaType;

    private int bufferSize;


    @Override
    public Function<RoutingContext, Future<?>> get() {

        Iterable<DidoData> data = Objects.requireNonNull(this.data, "Data is null");

        String mediaType = Objects.requireNonNullElse(this.mediaType, "application/octet-stream");

        DataOutHow<OutputStream> how = Objects.requireNonNull(this.how, "How is null");

        int bufferSize = this.bufferSize == 0 ? DEFAULT_BUFFER_SIZE : this.bufferSize;

        return new Function<>() {

            public Future<?> apply(RoutingContext routingContext) {

                ByteArrayOutputStream out = new ByteArrayOutputStream(bufferSize);

                try (DataOut dataOut = how.outTo(out)) {

                    data.forEach(dataOut);
                }

                return routingContext.response()
                                .putHeader("content-type", mediaType)
                                .end(Buffer.buffer(out.toByteArray()));
            }

            @Override
            public String toString() {
                return "DidoOutEndpoint " + mediaType;
            }
        };
    }

    public Iterable<DidoData> getData() {
        return data;
    }

    public void setData(Iterable<DidoData> data) {
        this.data = data;
    }

    public DataOutHow<OutputStream> getHow() {
        return how;
    }

    public void setHow(DataOutHow<OutputStream> how) {
        this.how = how;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public String getMediaType() {
        return mediaType;
    }

    public void setMediaType(String mediaType) {
        this.mediaType = mediaType;
    }

}
