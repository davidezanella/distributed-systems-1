package it.unitn.ds1.project;

import java.io.Serializable;

public class MsgAck implements Serializable {
    final UpdateKey key;
    public MsgAck(UpdateKey key) {
        this.key = key;
    }

    @Override
    public String toString() {
        return "ack";
    }
}
