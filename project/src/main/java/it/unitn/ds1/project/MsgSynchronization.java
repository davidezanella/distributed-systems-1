package it.unitn.ds1.project;

import java.io.Serializable;

public class MsgSynchronization implements Serializable {
    public final Integer id;
    public MsgSynchronization(Integer id){
        this.id = id;
    }

    @Override
    public String toString() {
        return "sync";
    }
}
