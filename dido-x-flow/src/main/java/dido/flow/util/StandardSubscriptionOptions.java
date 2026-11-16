package dido.flow.util;

import dido.flow.SubscriptionOptions;

public class StandardSubscriptionOptions {

    private boolean snapshot;

    private boolean delta;

    private StandardSubscriptionOptions() {}

    public static StandardSubscriptionOptions of() {
        return new StandardSubscriptionOptions();
    }

    public SubscriptionOptions snapshot() {
        return new Impl(true, false);
    }

    public SubscriptionOptions delta() {
        return new Impl(false, true);
    }

    public SubscriptionOptions all() {
        return new Impl(true, true);
    }

    static class Impl implements SubscriptionOptions {

        private final boolean snapshot;

        private final boolean delta;

        Impl(boolean snapshot,  boolean delta) {
            this.snapshot = snapshot;
            this.delta = delta;
        }

        @Override
        public boolean snapshot() {
            return snapshot;
        }

        @Override
        public boolean delta() {
            return delta;
        }
    }


}
