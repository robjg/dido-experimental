package dido.table;

import dido.operators.transform.FieldReader;

import java.util.function.Consumer;

public interface LiveValue extends MutableField {


    QuietlyCloseable onChange(Consumer<? super FieldReader> listener);

}
