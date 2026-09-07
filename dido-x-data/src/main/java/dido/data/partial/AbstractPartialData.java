package dido.data.partial;

abstract public class AbstractPartialData implements PartialData {

    @Override
    public String toString() {
        return PartialData.toString(this);
    }
}
