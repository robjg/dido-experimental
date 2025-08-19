package dido.table;

import dido.flow.QuietlyCloseable;
import dido.operators.transform.ValueGetter;
import dido.operators.transform.ValueSetter;

import java.util.function.Consumer;

public interface LiveValue extends ValueGetter, ValueSetter {

    QuietlyCloseable addChangeListener(Consumer<? super ValueGetter> listener);
}
