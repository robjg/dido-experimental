package dido.platform;

import dido.data.DidoData;

import java.util.function.Consumer;
import java.util.stream.Stream;

public interface Query extends AutoCloseable {

    interface Builder {

        Builder endpoint(String endpoint);

        Builder filter(Filter filter);

        Builder options(QueryOptions options);

        Builder to(Consumer<? super DidoData> to);

        Query start();

        Stream<DidoData> stream();
    }

    @Override
    void close();
}
