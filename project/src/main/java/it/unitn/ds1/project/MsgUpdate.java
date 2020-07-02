package it.unitn.ds1.project;

import java.io.Serializable;

public class MsgUpdate implements Serializable {
    public final String value;
    public final String requestId; // used only to link it to the original write request
    public final Integer e; // epoch number
    public final Integer i; // sequence number

    public MsgUpdate(String value, Integer e, Integer i, String requestId) {
        this.value = value;
        this.e = e;
        this.i = i;
        this.requestId = requestId;
    }
}
