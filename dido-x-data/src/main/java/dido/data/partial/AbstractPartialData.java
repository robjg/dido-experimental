package dido.data.partial;

abstract public class AbstractPartialData implements PartialData {

    @Override
    public int hashCode() {
        return PartialData.hashCode(this);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof PartialData other) {
            return PartialData.equals(this, other);
        }
        else {
            return false;
        }
    }

    @Override
    public String toString() {
        return PartialData.toString(this);
    }
}
