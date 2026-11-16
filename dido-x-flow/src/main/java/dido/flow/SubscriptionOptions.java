package dido.flow;

import dido.flow.util.StandardSubscriptionOptions;

public interface SubscriptionOptions {

    boolean snapshot();

    boolean delta();

    static StandardSubscriptionOptions of() {
        return StandardSubscriptionOptions.of();
    }
}
