package dido.platform.local;

import dido.platform.Platform;
import dido.platform.Publication;
import dido.platform.Query;
import dido.platform.Subscription;

public class LocalPlatform implements Platform {

    private final EndpointManager endpointManager = new EndpointManager();

    public static LocalPlatform create() {

        return new LocalPlatform();
    }

    @Override
    public Publication.Builder send() {
        return endpointManager.new PublicationBuilder();
    }

    @Override
    public Query.Builder query() {
        return endpointManager.new QueryBuilder();
    }

    @Override
    public Subscription.Builder subscribe() {
        return null;
    }
}
