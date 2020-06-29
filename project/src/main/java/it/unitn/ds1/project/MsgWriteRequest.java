package it.unitn.ds1.project;

import java.io.Serializable;

public class MsgWriteRequest implements Serializable {
    public final String newValue;
    public MsgWriteRequest(String newValue) {
        this.newValue = newValue;
    }
}
