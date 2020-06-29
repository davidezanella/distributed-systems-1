package it.unitn.ds1.project;

import java.io.Serializable;

public class MsgWriteValue implements Serializable {
    public final String newValue;
    public MsgWriteValue(String newValue) {
        this.newValue = newValue;
    }
}
