package it.unitn.ds1.project;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import scala.concurrent.duration.Duration;

import java.util.concurrent.TimeUnit;
import java.util.Random;
import java.io.Serializable;

public class Client extends AbstractActor {
    private ActorRef[] replicas;

    public Client(ActorRef[] replicas) {
        this.replicas = replicas;
    }

    static public Props props(ActorRef[] replicas) {
        return Props.create(Client.class, () -> new Client(replicas));
    }

    private int getIDRandomReplica() {
        int idx = new Random().nextInt(this.replicas.length);
        return idx;
    }

    private Serializable getNewRequest() {
        int coin = new Random().nextInt(2);
        if(coin == 0) {
            return new MsgWriteRequest("1234", null);
        } else {
            return new MsgReadRequest();
        }
    }

    @Override
    public void preStart() {
        
        // Create a timer that will periodically send a message to the receiver actor
        Cancellable timer = getContext().system().scheduler().scheduleWithFixedDelay(
                Duration.create(5, TimeUnit.SECONDS),               // when to start generating messages
                Duration.create(5, TimeUnit.SECONDS),               // how frequently generate them
                getSelf(),                  // destination actor reference
                new MsgSelf(), // the message to send
                getContext().system().dispatcher(),                 // system dispatcher
                getSelf()                                           // source of the message (myself)
        );
        
    }

    private void onMsgRWResponse(MsgRWResponse m) {
        System.out.println("[" +
                getSelf().path().name() +      // the name of the current actor
                "] received a message from " +
                getSender().path().name() +    // the name of the sender actor
                ": value: " + m.value
        );
    }

    private void onMsgSelf(MsgSelf m) {
        int delaySecs = 0;
        getContext().system().scheduler().scheduleOnce(
                Duration.create(delaySecs, TimeUnit.SECONDS),
                this.replicas[getIDRandomReplica()],
                getNewRequest(),
                getContext().system().dispatcher(),
                getSelf()
        );
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(MsgRWResponse.class, this::onMsgRWResponse)
                .match(MsgSelf.class, this::onMsgSelf)
                .build();
    }
}
