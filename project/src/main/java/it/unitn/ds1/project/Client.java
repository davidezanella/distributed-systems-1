package it.unitn.ds1.project;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import akka.event.DiagnosticLoggingAdapter;
import akka.event.Logging;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class Client extends AbstractActor {
    private ActorRef[] replicas;
    DiagnosticLoggingAdapter log = Logging.getLogger(this);

    public Client(ActorRef[] replicas) {
        Map<String, Object> mdc = new HashMap<String, Object>();
        mdc.put("elementId", getSelf().path().name());
        log.setMDC(mdc);
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
            return new MsgWriteRequest(Utils.generateRandomString(), null);
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

    private void onMsgReadResponse(MsgReadResponse m) {
        log.info("received " + m + " from " + getSender().path().name() + " - value: " + m.value);
    }

    private void onMsgWriteResponse(MsgWriteResponse m) {
        log.info("received " + m + " from " + getSender().path().name() + " - value: " + m.value);
    }

    private void onMsgSelf(MsgSelf m) {
        ActorRef dest = this.replicas[getIDRandomReplica()];
        Serializable req = getNewRequest();

        int delaySecs = 0;
        getContext().system().scheduler().scheduleOnce(
                Duration.create(delaySecs, TimeUnit.SECONDS),
                dest,
                req,
                getContext().system().dispatcher(),
                getSelf()
        );

        log.info("sent " + req + " to " + dest.path().name());
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(MsgReadResponse.class, this::onMsgReadResponse)
                .match(MsgWriteResponse.class, this::onMsgWriteResponse)
                .match(MsgSelf.class, this::onMsgSelf)
                .build();
    }
}
