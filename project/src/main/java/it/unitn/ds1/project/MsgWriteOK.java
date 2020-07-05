package it.unitn.ds1.project;

import java.io.Serializable;

public class MsgWriteOK implements Serializable, Comparable<MsgWriteOK>  {
    public final String value;
    public final Integer e; // epoch number
    public final Integer i; // sequence number
    public MsgWriteOK(String value, Integer e, Integer i) {
        this.e = e;
        this.i = i;
        this.value = value;
    }

    @Override
    public int compareTo(MsgWriteOK o) {
        int res = this.e.compareTo(o.e);
        if (res == 0) {
            res = this.i.compareTo(o.i);
        }
        return res;
    }

    @Override
    public String toString() {
        return "write_ok";
    }
}
