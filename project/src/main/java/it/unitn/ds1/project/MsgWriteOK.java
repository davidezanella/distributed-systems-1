package it.unitn.ds1.project;

import java.io.Serializable;

public class MsgWriteOK implements Serializable {
    public final String value;
    public MsgWriteOK(String value) {
        this.value = value;
    }
}
