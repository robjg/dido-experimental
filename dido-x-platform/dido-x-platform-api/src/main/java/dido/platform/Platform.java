package dido.platform;

/**
 * The main abstraction of a 'Platform'.
 * <p>
 *     How is this used? Some kind of service lookup?
 * </p>
 */
public interface Platform {

    String ENDPOINT_ENDPOINT = "ENDPOINTS";

    Publication.Builder send();

    Query.Builder query();

    Subscription.Builder subscribe();
}
