package it.unitn.ds1.project;

import java.io.Serializable;

public class MsgWriteRequest implements Serializable {
    public final String newValue;
    public final String requestId;
    public MsgWriteRequest(String newValue, String requestId) {
        this.requestId = requestId;
        this.newValue = newValue;
    }
}
