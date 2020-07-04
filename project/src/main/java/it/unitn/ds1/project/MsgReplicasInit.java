package it.unitn.ds1.project;

import akka.actor.ActorRef;

import java.io.Serializable;

public class MsgReplicasInit implements Serializable {
    public final ActorRef[] replicas;
    public MsgReplicasInit(ActorRef[] replicas) {
        this.replicas = replicas;
    }

    @Override
    public String toString() {
        return "replicas_init";
    }
}
