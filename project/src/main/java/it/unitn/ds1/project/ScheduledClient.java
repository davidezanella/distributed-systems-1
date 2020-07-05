package it.unitn.ds1.project;

import akka.actor.ActorRef;
import akka.actor.Props;

import java.io.Serializable;

public class ScheduledClient extends Client {
    private int cont_messages;

    public ScheduledClient(ActorRef[] replicas) {
        super(replicas);
        cont_messages = 0;
    }

    static public Props props(ActorRef[] replicas) {
        return Props.create(ScheduledClient.class, () -> new ScheduledClient(replicas));
    }

    @Override
    protected Serializable getNewRequest() {
        cont_messages = (cont_messages + 1) % 3;
        
        if(cont_messages % 3 == 1) {
            return new MsgWriteRequest(Utils.generateRandomString(), null);
        } else if(cont_messages % 3 == 2) {
            return new MsgReadRequest();
        } else {
            return new MsgCrash();
        }
    }
}

