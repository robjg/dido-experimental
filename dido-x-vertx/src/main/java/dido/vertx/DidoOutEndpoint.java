package dido.vertx;

import dido.data.DidoData;
import dido.how.DataOut;
import dido.how.DataOutHow;
import io.vertx.core.buffer.Buffer;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.function.Supplier;

public class DidoOutEndpoint implements Supplier<Buffer> {

    private static final int DEFAULT_BUFFER_SIZE = 256;

    private DataOutHow<OutputStream> how;

    private String mediaType;

    private int bufferSize;

    private Iterable<DidoData> data;

    @Override
    public Buffer get() {

        ByteArrayOutputStream out = new ByteArrayOutputStream(
                bufferSize == 0 ? DEFAULT_BUFFER_SIZE : bufferSize);

        try (DataOut dataOut = how.outTo(out)) {

            data.forEach(dataOut);
        }

        return Buffer.buffer(out.toByteArray());
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
