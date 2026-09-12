package dido.platform.local;


import dido.data.DataSchema;
import dido.data.DidoData;
import dido.platform.*;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class EndpointManager {

    private final Map<String, LocalEndpoint> endpoints = new ConcurrentHashMap<>();

    class PublicationBuilder implements Publication.Builder {

        private String endpoint;

        private DataSchema schema;

        @Override
        public PublicationBuilder endpoint(String endpoint) {
            this.endpoint = endpoint;
            return this;
        }

        @Override
        public PublicationBuilder schema(DataSchema schema) {
            this.schema = schema;
            return this;
        }

        @Override
        public Receiver start() {
            LocalEndpoint endpoint = endpoints.get(
                    Objects.requireNonNull(this.endpoint, "No Endpoint"));
            if (endpoint == null) {
                endpoint = LocalEndpoint.of(this.endpoint,
                        Objects.requireNonNull(this.schema, "No Schema"));
                endpoints.put(this.endpoint, endpoint);
            }
            return endpoint;
        }
    }

    class QueryBuilder implements Query.Builder {

        private String endpoint;

        private Consumer<? super DidoData> to;

        @Override
        public QueryBuilder endpoint(String endpoint) {
            this.endpoint = endpoint;
            return this;
        }

        @Override
        public QueryBuilder filter(Filter filter) {
            return this;
        }

        @Override
        public QueryBuilder options(QueryOptions options) {
            return this;
        }

        @Override
        public QueryBuilder to(Consumer<? super DidoData> to) {
            return null;
        }

        @Override
        public Query start() {
            stream().forEach(Objects.requireNonNull(this.to, "No To"));
            return () -> {
                // Nothing to free.
            };
        }

        @Override
        public Stream<DidoData> stream() {
            LocalEndpoint endpoint = Objects.requireNonNull(
                    endpoints.get(Objects.requireNonNull(this.endpoint, "No Endpoint")),
                    "No Endpoint " + this.endpoint);

            return endpoint.stream(null);
        }
    }



}
