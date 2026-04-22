package dido.vertx.util;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Consumer;

public class SectionTextOut {

    static final String PADDING = "  ";

    private final Appendable out;

    private int level;

    protected SectionTextOut(Appendable out) {
        this.out = out;
    }

    public static FormatterOut of(Appendable out) {
        return new SectionTextOut(out).new Root();
    }

    void write(String text) {
        try {
            out.append(text);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    abstract class Base implements FormatterOut  {

        void pad() {
            for (int i = 0; i < level; i++) {
                write(PADDING);
            }
        }

        @Override
        public FormatterOut value(String name, Object value) {
            pad();
            write(name);
            write(": ");
            write(String.valueOf(value));
            write("\n");
            return this;
        }

        @Override
        public FormatterOut nested(String name, Consumer<? super FormatterOut> consumer) {
            pad();
            write("---- " + name + " ----\n");
            ++level;
            consumer.accept(this);
            --level;
            return this;
        }

        @Override
        public FormatterOut repeating(String name, Consumer<? super Repeating> out) {
            pad();
            write("---- " + name + " -----\n");
            ++level;
            out.accept(new RepeatingSection());
            --level;
            return this;
        }

        @Override
        public FormatterOut out(Consumer<FormatterOut> consumer) {
            consumer.accept(this);
            return this;
        }
    }

    class Root extends Base {


    }

    class RepeatingSection implements  FormatterOut.Repeating {

        @Override
        public RepeatingSection item(Consumer<? super FormatterOut> out) {
            out.accept(new RepeatingItem());
            return this;
        }
    }

    class RepeatingItem extends Base {

        @Override
        void pad() {
            super.pad();
            write("- ");
        }
    }
}
