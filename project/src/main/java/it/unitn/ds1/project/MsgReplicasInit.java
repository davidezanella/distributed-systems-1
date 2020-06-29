package it.unitn.ds1.project;

import java.io.Serializable;
import akka.actor.ActorRef;

public class MsgReplicasInit implements Serializable {
    public final ActorRef[] replicas;
    public MsgReplicasInit(ActorRef[] replicas) {
        this.replicas = replicas;
    }
}
