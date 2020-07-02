package it.unitn.ds1.project;

import java.io.Serializable;

public class MsgWriteOK implements Serializable {
    public final String value;
    public final Integer e; // epoch number
    public final Integer i; // sequence number
    public MsgWriteOK(String value, Integer e, Integer i) {
        this.e = e;
        this.i = i;
        this.value = value;
    }
}
