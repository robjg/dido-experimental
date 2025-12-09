package dido.data.partial;

abstract public class AbstractPartialUpdate implements PartialUpdate{

    @Override
    public String toString() {
        return PartialUpdate.toString(this);
    }
}
