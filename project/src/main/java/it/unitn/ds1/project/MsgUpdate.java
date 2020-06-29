package it.unitn.ds1.project;

import java.io.Serializable;

public class MsgUpdate implements Serializable {
    public final String value;
    public final Integer e; // epoch number
    public final Integer i; // sequence number

    public MsgUpdate(String value, Integer e, Integer i) {
        this.value = value;
        this.e = e;
        this.i = i;
    }
}
