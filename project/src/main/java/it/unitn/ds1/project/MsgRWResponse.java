package it.unitn.ds1.project;

import java.io.Serializable;

public class MsgRWResponse implements Serializable {
    public final String value;
    public MsgRWResponse(String value) {
        this.value = value;
    }
}
