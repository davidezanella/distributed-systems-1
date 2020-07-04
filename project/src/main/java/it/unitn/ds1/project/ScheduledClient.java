package it.unitn.ds1.project;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import scala.concurrent.duration.Duration;

import java.util.concurrent.TimeUnit;
import java.util.Random;
import java.io.Serializable;

public class ScheduledClient extends Client {
    private int cont_messages = 0;

    public ScheduledClient(ActorRef[] replicas) {
        super(replicas);
    }

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

    protected void onMsgSelf(MsgSelf m) {
        int delaySecs = 0;
        getContext().system().scheduler().scheduleOnce(
                Duration.create(delaySecs, TimeUnit.SECONDS),
                this.replicas[getIDRandomReplica()],
                getNewRequest(),
                getContext().system().dispatcher(),
                getSelf()
        );
    }
}
