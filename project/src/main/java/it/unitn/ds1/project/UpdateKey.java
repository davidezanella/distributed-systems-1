package it.unitn.ds1.project;

public class UpdateKey {
    public Integer epoch;
    public Integer sequence;

    public UpdateKey(int e, int i) {
        this.epoch = e;
        this.sequence = i;
    }

    @Override
    public int hashCode() {
        return this.epoch * 1000 + this.sequence;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        UpdateKey key = (UpdateKey) obj;
        return (key.epoch.equals(this.epoch) && key.sequence.equals(this.sequence));
    }

    @Override
    public String toString() {
        return this.epoch + ":" + this.sequence;
    }
}
