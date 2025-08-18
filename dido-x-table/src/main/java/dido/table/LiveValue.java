package dido.table;

import dido.flow.QuietlyCloseable;
import dido.operators.transform.ValueGetter;

import java.util.function.Consumer;

public interface LiveValue extends MutableField {


    QuietlyCloseable onChange(Consumer<? super ValueGetter> listener);

}
