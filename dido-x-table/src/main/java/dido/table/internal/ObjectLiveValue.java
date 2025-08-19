package dido.table.internal;

import dido.flow.QuietlyCloseable;
import dido.operators.transform.ValueGetter;
import dido.table.LiveValue;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

public class ObjectLiveValue implements LiveValue {

    private final List<Consumer<? super ValueGetter>> listeners = new ArrayList<>();

    boolean changed;

    private Object value;

    @Override
    public boolean has() {
        return value != null;
    }

    @Override
    public Type getType() {
        return Object.class;
    }

    @Override
    public Object get() {
        return value;
    }

    @Override
    public void clear() {
        if (value != null) {
            changed = true;
        }
        set(null);
    }

    @Override
    public void set(Object value) {
        if (Objects.equals(this.value, value)) {
            changed = false;
        }
        else {
            changed = true;
            this.value = value;
            fireChange();
        }
    }


    @Override
    public QuietlyCloseable addChangeListener(Consumer<? super ValueGetter> listener) {
        listeners.add(listener);
        return () -> listeners.remove(listener);
    }

    protected void fireChange() {
        listeners.forEach(listener -> listener.accept(this));
    }
}
