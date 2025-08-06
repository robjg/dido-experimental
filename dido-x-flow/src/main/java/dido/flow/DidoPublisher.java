package dido.flow;

public interface DidoPublisher {

    QuietlyCloseable subscribe(Receiver subscriber);
}
