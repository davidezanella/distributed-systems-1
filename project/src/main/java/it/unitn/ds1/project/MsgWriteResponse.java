package it.unitn.ds1.project;

import java.io.Serializable;

public class MsgWriteResponse implements Serializable {
    public final String value;
    public MsgWriteResponse(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "write_resp";
    }
}
