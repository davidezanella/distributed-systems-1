package it.unitn.ds1.project;

import java.io.Serializable;

public class MsgUpdate implements Serializable {
    public final String value;
    public final String requestId; // used only to link it to the original write request
    public final UpdateKey key;

    public MsgUpdate(String value, UpdateKey key, String requestId) {
        this.value = value;
        this.key = key;
        this.requestId = requestId;
    }

    @Override
    public String toString() {
        return "update";
    }
}
