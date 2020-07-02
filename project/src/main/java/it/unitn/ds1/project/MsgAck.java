package it.unitn.ds1.project;

import java.io.Serializable;

public class MsgAck implements Serializable {
    public final Integer e; // epoch number
    public final Integer i; // sequence number
    public MsgAck(Integer e, Integer i) {
        this.e = e;
        this.i = i;
    }
}
