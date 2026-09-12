package dido.platform.local;


import dido.data.DataSchema;
import dido.data.DidoData;
import dido.data.partial.PartialData;
import dido.flow.util.KeyUtil;
import dido.platform.Endpoint;
import dido.platform.Filter;
import dido.platform.Receiver;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Stream;

public class LocalEndpoint implements Endpoint, Receiver {

    private final String name;

    private final Function<DidoData, Object> indexExtractor;

    private final Map<Object, DidoData> data = new ConcurrentHashMap<>();

    private final List<Receiver> consumers = new ArrayList<>();

    private LocalEndpoint(String name, Function<DidoData, Object> indexExtractor) {
        this.name = Objects.requireNonNull(name);
        this.indexExtractor = indexExtractor;
    }

    public static LocalEndpoint of(String name, DataSchema schema) {
        return new LocalEndpoint(name, KeyUtil.fromFirstField(schema));
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void onData(DidoData data) {
        this.data.put(indexExtractor.apply(data), data);
        for (Receiver receiver : consumers) {
            receiver.onData(data);
        }
    }

    @Override
    public void onPartial(PartialData partial) {

    }

    @Override
    public void onDelete(DidoData partial) {

    }

    @Override
    public void close() {

    }

    void addSubscriber() {

    }

    Stream<DidoData> stream(Filter filter) {
        return data.values().stream();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        LocalEndpoint that = (LocalEndpoint) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name);
    }

    @Override
    public String toString() {
        return "Endpoint: " + name;
    }
}
