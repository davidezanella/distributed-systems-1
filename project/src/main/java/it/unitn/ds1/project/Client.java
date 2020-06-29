package it.unitn.ds1.project;

import scala.concurrent.duration.Duration;
import java.util.concurrent.TimeUnit;
import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.Cancellable;

public class Client extends AbstractActor {
    private ActorRef[] replicas;

    public Client(ActorRef[] replicas) {
        this.replicas = replicas;
    }

    static public Props props(ActorRef[] replicas) {
        return Props.create(Client.class, () -> new Client(replicas));
    }

    @Override
    public void preStart() {

        // Create a timer that will periodically send a message to the receiver actor
        Cancellable timer = getContext().system().scheduler().scheduleWithFixedDelay(
                Duration.create(1, TimeUnit.SECONDS),               // when to start generating messages
                Duration.create(1, TimeUnit.SECONDS),               // how frequently generate them
                this.replicas[0],                                           // destination actor reference
                new MsgReadValue(), // the message to send
                getContext().system().dispatcher(),                 // system dispatcher
                getSelf()                                           // source of the message (myself)
        );
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder().build(); // this actor does not handle any incoming messages
    }
}
