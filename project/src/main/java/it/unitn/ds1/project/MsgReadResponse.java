package it.unitn.ds1.project;

import java.io.Serializable;

public class MsgReadResponse implements Serializable {
    public final String value;
    public MsgReadResponse(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "read_resp";
    }
}
