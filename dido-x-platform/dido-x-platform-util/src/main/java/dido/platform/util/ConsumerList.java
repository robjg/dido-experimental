package dido.platform.util;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class ConsumerList<T> implements Consumer<T> {

    private final List<Consumer<? super T>> consumers = new ArrayList<>();

    private ConsumerList() {

    }

    public static <T> void maybeReplace(Consumer<? super T> existing,
                                        Consumer<? super T> additional,
                                        Consumer<Consumer<? super T>> replacer) {
        if (existing == null) {
            replacer.accept(additional);
        }
        else if (existing instanceof ConsumerList consumerList) {
            consumerList.consumers.add(additional);
        }
        else {
            ConsumerList<T> consumerList = new ConsumerList<>();
            consumerList.consumers.add(existing);
            consumerList.consumers.add(additional);
            replacer.accept(consumerList);
        }
    }


    @Override
    public void accept(T t) {
        consumers.forEach(c -> c.accept(t));
    }
}
