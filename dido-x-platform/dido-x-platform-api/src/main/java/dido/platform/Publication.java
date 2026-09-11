package dido.platform;

import dido.data.DataSchema;

/**
 * Provide a means of sending data to an endpoint.
 * <p>
 * Badly Named.
 */
public interface Publication {

    interface Builder {

        Builder endpoint(String endpoint);

        Builder schema(DataSchema schema);

        Receiver start();
    }


}
