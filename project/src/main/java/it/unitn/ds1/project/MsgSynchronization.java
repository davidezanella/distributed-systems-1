package it.unitn.ds1.project;

import java.io.Serializable;
import java.util.concurrent.PriorityBlockingQueue;

public class MsgSynchronization implements Serializable {
    public final Integer id;
    public final PriorityBlockingQueue<MsgWriteOK> missingUpdates = new PriorityBlockingQueue<>();
    public MsgSynchronization(Integer id){
        this.id = id;
    }

    @Override
    public String toString() {
        return "sync";
    }
}
