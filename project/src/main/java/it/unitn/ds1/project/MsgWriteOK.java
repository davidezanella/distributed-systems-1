package it.unitn.ds1.project;

import java.io.Serializable;

public class MsgWriteOK implements Serializable {
    public final String value;
    public final String requestId;
    public MsgWriteOK(String value, String requestId) {
        this.requestId = requestId;
        this.value = value;
    }
}
