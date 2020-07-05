package it.unitn.ds1.project;

import java.io.Serializable;

public class MsgWriteOK implements Serializable, Comparable<MsgWriteOK>  {
    public final String value;
    public final UpdateKey key;
    public MsgWriteOK(String value, UpdateKey key) {
        this.key = key;
        this.value = value;
    }

    @Override
    public int compareTo(MsgWriteOK o) {
        int res = this.key.epoch.compareTo(o.key.epoch);
        if (res == 0) {
            res = this.key.sequence.compareTo(o.key.sequence);
        }
        return res;
    }

    @Override
    public String toString() {
        return "write_ok";
    }
}
