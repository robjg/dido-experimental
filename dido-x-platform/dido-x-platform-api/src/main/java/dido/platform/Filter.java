package dido.platform;

public interface Filter {

    interface Builder {

        Filter fromString(String string);
    }
}
