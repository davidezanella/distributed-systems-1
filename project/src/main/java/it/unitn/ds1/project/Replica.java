package it.unitn.ds1.project;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import scala.concurrent.duration.Duration;

import javax.swing.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

public class Replica extends AbstractActor {
    final private Integer TIMEOUT = 10000; // Timeout value in milliseconds
    //TODO: add more to be able to simulate every different timeout situation
    final private Double PROB_OF_CRASH = 0.2; // Probability that the replica will crash while sending a message
    final private Integer MAX_RESP_DELAY = 4; // Maximum delay in seconds while sending a message

    private String v = "init";
    private boolean crashed = false;
    private Integer coordinatorIdx = 0; //TODO: implement Coordinator election
    private Integer sequenceNumber = 0;
    private Integer epochNumber = 0;
    final private Integer id;
    private ActorRef[] replicas;

    private final HashMap<String, Integer> AckReceived = new HashMap<>(); // Used in the UPDATE phase
    private final HashMap<String, String> pendingUpdates = new HashMap<>(); // waiting for quorum
    private final ArrayList<MsgWriteOK> updatesHistory = new ArrayList<>();

    private final HashMap<String, Timer> timersWriteOk = new HashMap<>();
    private final HashMap<String, Timer> timersBroadcastInit = new HashMap<>();
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
        if (this.crashed)
            return;

        System.out.println("[" +
                getSelf().path().name() +      // the name of the current actor
                "] received a message from " +
                getSender().path().name() +    // the name of the sender actor
                ": read request"
        );

        // send to the client a message with the value of v
        sendOneMessageOrCrash(getSender(), new MsgRWResponse(v));
    }

    private void onMsgWriteRequest(MsgWriteRequest m) {
        if (this.crashed)
            return;

        System.out.println("[" +
                getSelf().path().name() +      // the name of the current actor
                "] received a message from " +
                getSender().path().name() +    // the name of the sender actor
                ": write request, v: " + m.newValue
        );

        if (this.id.equals(this.coordinatorIdx)) {
            System.out.println("Coordinator received an update request");

            this.sequenceNumber++;

            String key = this.epochNumber + "-" + this.sequenceNumber;
            this.AckReceived.put(key, 0);
            this.pendingUpdates.put(key, m.newValue);
            final MsgUpdate update = new MsgUpdate(m.newValue, this.epochNumber, this.sequenceNumber, m.requestId);

            // Send a broadcast to all the replicas
            broadcastToReplicas(update);
        } else {
            String requestId = Utils.generateRandomString();
            MsgWriteRequest req = new MsgWriteRequest(m.newValue, requestId);
            // forward the request to the coordinator
            boolean sent = sendOneMessageOrCrash(replicas[coordinatorIdx], req);

            if (sent) {
                Timer timerBroadcastInit = new Timer(this.TIMEOUT, actionTimeoutExceeded);
                timerBroadcastInit.setRepeats(false);
                this.timersBroadcastInit.put(requestId, timerBroadcastInit);
                timerBroadcastInit.start();
            }
        }
    }

    private void onMsgUpdate(MsgUpdate m) {
        if (this.crashed)
            return;

        if (this.timersBroadcastInit.containsKey(m.requestId)) {
            this.timersBroadcastInit.get(m.requestId).stop();
            this.timersBroadcastInit.remove(m.requestId);
        }

        // respond to the coordinator with an ACK
        MsgAck ack = new MsgAck(m.e, m.i);
        boolean sent = sendOneMessageOrCrash(getSender(), ack);

        if (sent) {
            Timer timerWriteOk = new Timer(this.TIMEOUT, actionTimeoutExceeded);
            timerWriteOk.setRepeats(false);
            String key = m.e + "-" + m.i;
            this.timersWriteOk.put(key, timerWriteOk);
            timerWriteOk.start();
        }
    }

    private void onMsgAck(MsgAck m) throws Exception {
        if (this.crashed)
            return;

        if (!this.id.equals(this.coordinatorIdx)) {
            throw new Exception("Not coordinator replica receives ACK message!");
        }

        String key = m.e + "-" + m.i;
        if (this.AckReceived.containsKey(key)) {
            int num = this.AckReceived.get(key) + 1;
            this.AckReceived.put(key, num);
            if (num >= Math.floor(this.replicas.length / 2.0) + 1) {
                final MsgWriteOK okMsg = new MsgWriteOK(this.pendingUpdates.get(key), m.e, m.i);
                broadcastToReplicas(okMsg);
                this.AckReceived.remove(key);
            }
        }
    }

    private void onMsgWriteOK(MsgWriteOK m) {
        if (this.crashed)
            return;

        String key = m.e + "-" + m.i;
        if (this.timersWriteOk.containsKey(key)) {
            this.timersWriteOk.get(key).stop();
            this.timersWriteOk.remove(key);

            System.out.println("[" +
                getSelf().path().name() +
                "] write value, v: " + m.value
            );
            this.v = m.value;

            // store in the history the write
            this.updatesHistory.add(m);
        }
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
        for (ActorRef replica : this.replicas) {
            boolean sent = sendOneMessageOrCrash(replica, message);
            if (!sent)
                return;
        }
    }

    private boolean sendOneMessageOrCrash(ActorRef dest, Serializable msg) {
        if (this.crashed)
            return false;

        if (Math.random() < PROB_OF_CRASH) {
            System.out.println("[" +
                    getSelf().path().name() +      // the name of the current actor
                    "] CRASHED!"
            );
            this.crashed = true;
            return false;
        }

        int delaySecs = (int) (Math.random() * (MAX_RESP_DELAY + 1));

        getContext().system().scheduler().scheduleOnce(
                Duration.create(delaySecs, TimeUnit.SECONDS),
                dest,
                msg,
                getContext().system().dispatcher(),
                getSelf()
        );
        return true;
    }

    ActionListener actionTimeoutExceeded = new ActionListener() {
        public void actionPerformed(ActionEvent actionEvent) {
            System.out.println("Timeout exceeded!");
            //TODO: Coordinator election
        }
    };

    ActionListener actionSendHeartbeat = new ActionListener() {
        public void actionPerformed(ActionEvent actionEvent) {
            broadcastToReplicas(new MsgHeartbeat());
        }
    };

    // emulate a delay of d milliseconds
    void delay(int d) {
        try {
            Thread.sleep(d);
        } catch (Exception ignored) {
        }
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
