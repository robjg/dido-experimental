package dido.vertx.util;

import java.util.function.Consumer;

public interface FormatterOut {

    FormatterOut value(String name, Object value);

    FormatterOut nested(String name, Consumer<? super FormatterOut> out);

    FormatterOut repeating(String name, Consumer<? super Repeating> out);

    FormatterOut out(Consumer<FormatterOut> consumer);

    interface Repeating {

        Repeating item(Consumer<? super FormatterOut> out);
    }

}
