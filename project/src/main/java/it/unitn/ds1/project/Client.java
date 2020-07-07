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
    ActorRef[] replicas; // Array conteining references to replicas
    DiagnosticLoggingAdapter log = Logging.getLogger(this); // Logger
    final private Integer MAX_RESP_DELAY = 4; // Maximum delay in seconds while sending a message
    
    // Debug purpose
    private Integer defaultReplica;
    private boolean keepSameReplica = false;

    public Client(ActorRef[] replicas) {
        Map<String, Object> mdc = new HashMap<String, Object>();
        mdc.put("elementId", getSelf().path().name());
        log.setMDC(mdc);
        this.replicas = replicas;
    }

    static public Props props(ActorRef[] replicas) {
        return Props.create(Client.class, () -> new Client(replicas));
    }

    // Randomly returns the ID of a replica (used to target a request)
    protected int getIDRandomReplica() {
        int idx = new Random().nextInt(this.replicas.length);
        if (defaultReplica == null)
        defaultReplica = idx;
        if (keepSameReplica)
            return defaultReplica;
        return idx;
    }

    // Randomly returns a request of type Read of Write
    protected Serializable getNewRequest() {
        int coin = new Random().nextInt(2);
        if(coin == 0) {
            // Updating request with a randomly generated string
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

    // Method to handle responses to read requests (logging)
    protected void onMsgReadResponse(MsgReadResponse m) {
        log.info("received " + m + " from " + getSender().path().name() + " - value: " + m.value);
    }

    // Method to handle responses to write requests (logging)
    protected void onMsgWriteResponse(MsgWriteResponse m) {
        log.info("received " + m + " from " + getSender().path().name() + " - value: " + m.value);
    }

    // Method to handle responses to self-sended messages
    protected void onMsgSelf(MsgSelf m) {
        // Methods to get the destination replica and the request are called
        ActorRef dest = this.replicas[getIDRandomReplica()];
        Serializable req = getNewRequest();
        
        // Generate delay
        int delaySecs = (int) (Math.random() * MAX_RESP_DELAY);

        // Scheduling of the request
        getContext().system().scheduler().scheduleOnce(
                Duration.create(delaySecs, TimeUnit.SECONDS),
                dest,
                req,
                getContext().system().dispatcher(),
                getSelf()
        );

        // logging
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
