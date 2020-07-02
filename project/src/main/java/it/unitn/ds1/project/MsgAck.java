package it.unitn.ds1.project;

import java.io.Serializable;

public class MsgAck implements Serializable {
    public final String requestId;
    public MsgAck(String requestId) {
        this.requestId = requestId;
    }
}
