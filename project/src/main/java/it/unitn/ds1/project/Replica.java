package it.unitn.ds1.project;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import scala.concurrent.duration.Duration;

import javax.swing.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.Serializable;
import java.util.concurrent.TimeUnit;

public class Replica extends AbstractActor {
    private String v = "init";
    private Boolean isCoordinator = false;
    private Integer coordinatorIdx = 0; //TODO: implement Coordinator election
    private Integer sequenceNumber = 0;
    private Integer epochNumber = 0;
    final private Integer id;
    private ActorRef[] replicas;

    private Integer AckReceived = 0; // Used in the UPDATE phase
    private String updateValue;

    final private Integer TIMEOUT = 5000; // Timeout value in milliseconds
    private Timer timerWriteOk; //TODO: maybe convert single timers to arraylist to have one timer for every message
    private Timer timerBroadcastInit;
    private String requestId;
    private Timer timerHeartbeat;

    public Replica(Integer id) {
        this.id = id;
    }

    // Some stuff required by Akka to create actors of this type
    static public Props props(Integer id) {
        return Props.create(Replica.class, () -> new Replica(id));
    }

    private void onMsgReplicasInit(MsgReplicasInit m) {
        this.replicas = m.replicas;
    }

    private void onMsgReadRequest(MsgReadRequest m) {
        System.out.println("[" +
                getSelf().path().name() +      // the name of the current actor
                "] received a message from " +
                getSender().path().name() +    // the name of the sender actor
                ": read request"
        );

        // send to the client a message with the value of v
        getContext().system().scheduler().scheduleOnce(
                Duration.create(2, TimeUnit.SECONDS),
                getSender(),
                new MsgRWResponse(v),
                getContext().system().dispatcher(),
                getSelf()
        );
    }

    private void onMsgWriteRequest(MsgWriteRequest m) {
        System.out.println("[" +
                getSelf().path().name() +      // the name of the current actor
                "] received a message from " +
                getSender().path().name() +    // the name of the sender actor
                ": write request, v: " + m.newValue
        );

        if (this.id.equals(this.coordinatorIdx)){//(this.isCoordinator) {
            System.out.println("Coordinator received an update request");

            this.AckReceived = 0;
            this.updateValue = m.newValue;
            this.sequenceNumber++;
            final MsgUpdate update = new MsgUpdate(m.newValue, this.epochNumber, this.sequenceNumber, m.requestId);

            // Send a broadcast to all the replicas
            broadcastToReplicas(update);
        }
        else {
            this.requestId = Utils.generateRandomString();
            MsgWriteRequest req = new MsgWriteRequest(m.newValue, this.requestId);
            // forward the request to the coordinator
            getContext().system().scheduler().scheduleOnce(
                    Duration.create(1, TimeUnit.SECONDS),
                    replicas[coordinatorIdx],
                    req,
                    getContext().system().dispatcher(),
                    getSelf()
            );

            this.timerBroadcastInit = new Timer(this.TIMEOUT, actionTimeoutExceeded);
            this.timerBroadcastInit.setRepeats(false);
            this.timerBroadcastInit.start();
        }
    }

    private void onMsgUpdate(MsgUpdate m) {
        if (m.requestId.equals(this.requestId))
            this.timerBroadcastInit.stop();

        // respond to the coordinator with an ACK
        getContext().system().scheduler().scheduleOnce(
                Duration.create(1, TimeUnit.SECONDS),
                getSender(),
                new MsgAck(),
                getContext().system().dispatcher(),
                getSelf()
        );

        this.timerWriteOk = new Timer(this.TIMEOUT, actionTimeoutExceeded);
        this.timerWriteOk.setRepeats(false);
        this.timerWriteOk.start();
    }

    private void onMsgAck(MsgAck m) throws Exception {
        if(!this.id.equals(this.coordinatorIdx)) {//(!this.isCoordinator) {
            throw new Exception("Not coordinator replica receives ACK message!");
        }

        if (this.AckReceived != null) {
            this.AckReceived++;
            if (this.AckReceived >= Math.floor(this.replicas.length / 2.0) + 1) {
                final MsgWriteOK okMsg = new MsgWriteOK(this.updateValue);
                broadcastToReplicas(okMsg);
                this.AckReceived = null;
            }
        }
    }

    private void onMsgWriteOK(MsgWriteOK m) {
        this.timerWriteOk.stop();
        System.out.println("[" +
                getSelf().path().name() +
                "] write value, v: " + m.value
        );
        this.v = m.value;
    }

    private void onMsgHeartbeat(MsgHeartbeat m) {
        /*
        Every time it receives a heartbeat from the coordinator, it stops the previous timer and initiates a new one.
        */
        this.timerHeartbeat.stop();

        this.timerHeartbeat = new Timer(this.TIMEOUT, actionTimeoutExceeded);
        this.timerHeartbeat.setRepeats(false);
        this.timerHeartbeat.start();
    }

    private void broadcastToReplicas(Serializable message) {
        for(ActorRef replica : this.replicas) {
            getContext().system().scheduler().scheduleOnce(
                    Duration.create(1, TimeUnit.SECONDS),
                    replica,
                    message,
                    getContext().system().dispatcher(),
                    getSelf()
            );
        }
    }

    ActionListener actionTimeoutExceeded = new ActionListener() {
        public void actionPerformed(ActionEvent actionEvent) {
            System.out.println( "Timeout exceeded!" );
            //TODO: Coordinator election
        }
    };

    // emulate a delay of d milliseconds
    void delay(int d) {
        try {Thread.sleep(d);} catch (Exception ignored) {}
    }

    // Here we define the mapping between the received message types
    // and our actor methods
    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(MsgReplicasInit.class, this::onMsgReplicasInit)
                .match(MsgUpdate.class, this::onMsgUpdate)
                .match(MsgAck.class, this::onMsgAck)
                .match(MsgWriteOK.class, this::onMsgWriteOK)
                .match(MsgReadRequest.class, this::onMsgReadRequest)
                .match(MsgWriteRequest.class, this::onMsgWriteRequest)
                .match(MsgHeartbeat.class, this::onMsgHeartbeat).build();
    }
}
