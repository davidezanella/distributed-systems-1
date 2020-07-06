package it.unitn.ds1.project;

import java.io.Serializable;
import java.util.concurrent.PriorityBlockingQueue;

public class MsgSynchronization implements Serializable {
    public final Integer id;
    public final Integer epoch;
    public final PriorityBlockingQueue<MsgWriteOK> missingUpdates = new PriorityBlockingQueue<>();
    public MsgSynchronization(Integer id, Integer epoch){
        this.id = id;
        this.epoch = epoch;
    }

    @Override
    public String toString() {
        return "sync";
    }
}
