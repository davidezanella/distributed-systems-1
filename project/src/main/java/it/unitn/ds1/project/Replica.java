package it.unitn.ds1.project;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.event.DiagnosticLoggingAdapter;
import akka.event.Logging;
import scala.concurrent.duration.Duration;

import javax.swing.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class Replica extends AbstractActor {
    DiagnosticLoggingAdapter log = Logging.getLogger(this);

    final private Integer TIMEOUT = 10000; // Timeout value in milliseconds
    final private Integer MAX_RESP_DELAY = 0; // Maximum delay in seconds while sending a message

    private String v = "init";
    private boolean crashed = false;
    private Integer coordinatorIdx;
    private Integer sequenceNumber = 0;
    private Integer epochNumber = 0;
    final private Integer id;
    private ActorRef[] replicas;

    private final HashMap<String, Integer> AckReceived = new HashMap<>(); // Used in the UPDATE phase
    private final HashMap<String, String> pendingUpdates = new HashMap<>(); // waiting for quorum
    private final ArrayList<MsgWriteOK> updatesHistory = new ArrayList<>();

    private final HashMap<String, Timer> timersWriteOk = new HashMap<>();
    private final HashMap<String, Timer> timersBroadcastInit = new HashMap<>();
    private Timer timerCoordinatorHeartbeat;
    private Timer timerHeartbeat;
    private Timer timerElection;

    public Replica(Integer id) {
        Map<String, Object> mdc = new HashMap<String, Object>();
        mdc.put("elementId", getSelf().path().name());
        log.setMDC(mdc);
        this.id = id;
    }

    // Some stuff required by Akka to create actors of this type
    static public Props props(Integer id) {
        return Props.create(Replica.class, () -> new Replica(id));
    }

    private void onMsgReplicasInit(MsgReplicasInit m) {
        this.replicas = m.replicas;
        if (this.coordinatorIdx == null) {
            // init coordinator election
            actionHeartbeatTimeoutExceeded.actionPerformed(null);
        }
    }

    private void onMsgReadRequest(MsgReadRequest m) {
        if (this.crashed)
            return;

        log.info("received " + m + " from " + getSender().path().name());

        // send to the client a message with the value of v
        sendOneMessage(getSender(), new MsgReadResponse(v));
    }

    private void onMsgWriteRequest(MsgWriteRequest m) {
        if (this.crashed)
            return;

        log.info("received " + m + " from " + getSender().path().name() + " - value: " + m.newValue);

        if (this.id.equals(this.coordinatorIdx)) {
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
            boolean sent = sendOneMessage(replicas[coordinatorIdx], req);

            if (sent) {
                Timer timerBroadcastInit = new Timer(this.TIMEOUT, actionBroadcastTimeoutExceeded);
                timerBroadcastInit.setRepeats(false);
                this.timersBroadcastInit.put(requestId, timerBroadcastInit);
                timerBroadcastInit.start();
            }
        }
    }

    private void onMsgUpdate(MsgUpdate m) {
        if (this.crashed)
            return;

        log.info("received " + m + " from " + getSender().path().name() + " " + m.e + ":" + m.i + " - value: " + m.value);

        if (this.timersBroadcastInit.containsKey(m.requestId)) {
            this.timersBroadcastInit.get(m.requestId).stop();
            this.timersBroadcastInit.remove(m.requestId);
        }

        // respond to the coordinator with an ACK
        MsgAck ack = new MsgAck(m.e, m.i);
        boolean sent = sendOneMessage(getSender(), ack);

        if (sent) {
            Timer timerWriteOk = new Timer(this.TIMEOUT, actionWriteOKTimeoutExceeded);
            timerWriteOk.setRepeats(false);
            String key = m.e + "-" + m.i;
            this.timersWriteOk.put(key, timerWriteOk);
            timerWriteOk.start();
        }
    }

    private void onMsgAck(MsgAck m) throws Exception {
        if (this.crashed)
            return;

        log.info("received " + m + " from " + getSender().path().name() + " " + m.e + ":" + m.i);

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

        log.info("received " + m + " from " + getSender().path().name() + " " + m.e + ":" + m.i + " - value: " + m.value);

        String key = m.e + "-" + m.i;
        if (this.timersWriteOk.containsKey(key)) {
            this.timersWriteOk.get(key).stop();
            this.timersWriteOk.remove(key);
            this.v = m.value;

            // store in the history the write
            this.updatesHistory.add(m);
        }
    }

    private void onMsgHeartbeat(MsgHeartbeat m) {
        /*
        Every time it receives a heartbeat from the coordinator, it stops the previous timer and initiates a new one.
        */
        log.info("received " + m + " from " + getSender().path().name());

        if(this.timerHeartbeat != null)
            this.timerHeartbeat.stop();

        this.timerHeartbeat = new Timer(this.TIMEOUT, actionHeartbeatTimeoutExceeded);
        this.timerHeartbeat.setRepeats(false);
        this.timerHeartbeat.start();
    }

    private ActorRef getNextReplica(Integer idx) {
        return replicas[(id + idx + 1) % replicas.length];
    }

    private void broadcastToReplicas(Serializable message) {
        for (ActorRef replica : this.replicas) {
            boolean sent = sendOneMessage(replica, message);
            if (!sent)
                return;
        }
    }

    private boolean sendOneMessage(ActorRef dest, Serializable msg) {
        if (this.crashed)
            return false;

        log.info("sent " + msg + " to " + dest.path().name());

        int delaySecs = (int) (Math.random() * MAX_RESP_DELAY);

        getContext().system().scheduler().scheduleOnce(
                Duration.create(delaySecs, TimeUnit.SECONDS),
                dest,
                msg,
                getContext().system().dispatcher(),
                getSelf()
        );
        return true;
    }

    ActionListener actionWriteOKTimeoutExceeded = new ActionListener() {
        public void actionPerformed(ActionEvent actionEvent) {
            log.info("timeout WriteOK");

            startCoordinatorElection();
        }
    };

    ActionListener actionBroadcastTimeoutExceeded = new ActionListener() {
        public void actionPerformed(ActionEvent actionEvent) {
            log.info("timeout Broadcast");

            startCoordinatorElection();
        }
    };

    ActionListener actionHeartbeatTimeoutExceeded = new ActionListener() {
        public void actionPerformed(ActionEvent actionEvent) {
            log.info("timeout Heartbeat");

            startCoordinatorElection();
        }
    };

    void startCoordinatorElection() {
        MsgElection election = new MsgElection();
        election.nodesHistory.put(id, updatesHistory);
        nextReplicaTry = 0;
        ActorRef nextReplica = getNextReplica(nextReplicaTry);
        sendOneMessage(nextReplica, election);

        messageToSend = election;

        this.timerElection = new Timer(this.TIMEOUT, actionElectionTimeout);
        this.timerElection.setRepeats(false);
        this.timerElection.start();
    }

    private Integer nextReplicaTry = 0;
    private Serializable messageToSend;
    private void onMsgElection(MsgElection m) {
        log.info("received " + m + " from " + getSender().path().name());

        sendOneMessage(getSender(), new MsgElectionAck());

        nextReplicaTry = 0;
        ActorRef nextReplica = getNextReplica(nextReplicaTry);

        // I'm already in the message, change message type
        if (m.nodesHistory.containsKey(id)) {
            // forward the coordinator message
            MsgCoordinator coord = new MsgCoordinator();
            coord.nodesHistory = m.nodesHistory;
            sendOneMessage(nextReplica, coord);
        } else {
            m.nodesHistory.put(id, updatesHistory);
            sendOneMessage(nextReplica, m);

            messageToSend = m;

            if (this.timerElection != null && this.timerElection.isRunning())
                this.timerElection.stop();
            this.timerElection = new Timer(this.TIMEOUT, actionElectionTimeout);
            this.timerElection.setRepeats(false);
            this.timerElection.start();
        }
    }

    private void onMsgElectionAck(MsgElectionAck m) {
        log.info("received " + m + " from " + getSender().path().name());

        if (this.timerElection != null && this.timerElection.isRunning())
            this.timerElection.stop();
    }

    ActionListener actionElectionTimeout = new ActionListener() {
        public void actionPerformed(ActionEvent actionEvent) {
            ActorRef previousReplica = getNextReplica(nextReplicaTry);

            log.info("timeout Election contacting " + previousReplica.path().name());

            nextReplicaTry++;
            ActorRef nextReplica = getNextReplica(nextReplicaTry);

            sendOneMessage(nextReplica, messageToSend);

            timerElection = new Timer(TIMEOUT, actionElectionTimeout);
            timerElection.setRepeats(false);
            timerElection.start();
        }
    };

    private void onMsgCoordinator(MsgCoordinator m) {
        log.info("received " + m + " from " + getSender().path().name());

        ActorRef nextReplica = getNextReplica(0);

        // decide the new coordinator and, if necessary, forward the coordinator message
        ArrayList<Integer> ids = new ArrayList<>(m.nodesHistory.keySet());
        ids.sort(new Comparator<Integer>() {
            @Override
            public int compare(Integer id1, Integer id2) {
                ArrayList<MsgWriteOK> history1 = m.nodesHistory.get(id1);
                ArrayList<MsgWriteOK> history2 = m.nodesHistory.get(id2);
                int res = 0;
                if (history1.size() > 0 && history2.size() > 0) {
                    MsgWriteOK ls1 = history1.get(history1.size() - 1);
                    MsgWriteOK ls2 = history2.get(history2.size() - 1);
                    res = ls1.e.compareTo(ls2.e) * -1;
                    if (res == 0) {
                        res = ls1.i.compareTo(ls2.i) * -1;
                    }
                }
                if (res == 0) {
                    res = id1.compareTo(id2) * -1;
                }
                return res;
            }
        });

        Integer newCoord = ids.get(0);

        if (!newCoord.equals(coordinatorIdx)) {
            sendOneMessage(nextReplica, m);

            if (newCoord.equals(id)) {
                // set up the new coordinator

                if (this.updatesHistory.size() > 0) {
                    this.epochNumber = this.updatesHistory.get(this.updatesHistory.size() - 1).e + 1;
                    this.sequenceNumber = 0;
                }

                if (this.timerHeartbeat != null)
                    this.timerHeartbeat.stop(); //not needed since now it's the coordinator

                this.timerCoordinatorHeartbeat = new Timer(this.TIMEOUT / 4, actionSendHeartbeat);
                this.timerCoordinatorHeartbeat.setRepeats(true);
                this.timerCoordinatorHeartbeat.start();

                // Send SYNCHRONIZATION message and sync replicas
                broadcastToReplicas(new MsgSynchronization(this.id));

                for (int repId: m.nodesHistory.keySet()) {
                    for (int i = 0; i < this.updatesHistory.size(); i++){
                        if (i >= m.nodesHistory.get(repId).size()){
                            sendOneMessage(this.replicas[repId], this.updatesHistory.get(i));
                        }
                    }
                }
            }
        }
    }

    private void onMsgSynchronization(MsgSynchronization m) {
        log.info("received " + m + " from " + getSender().path().name() + " - coordId: " + m.id);

        coordinatorIdx = m.id;

        onMsgHeartbeat(null);
    }

    ActionListener actionSendHeartbeat = new ActionListener() {
        public void actionPerformed(ActionEvent actionEvent) {
            broadcastToReplicas(new MsgHeartbeat());
        }
    };

    private void onMsgCrash(MsgCrash m) {
        log.info("received " + m + " from " + getSender().path().name());

        this.crashed = true;
    }

    // Here we define the mapping between the received message types
    // and our actor methods
    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(MsgCrash.class, this::onMsgCrash)
                .match(MsgReplicasInit.class, this::onMsgReplicasInit)
                .match(MsgUpdate.class, this::onMsgUpdate)
                .match(MsgAck.class, this::onMsgAck)
                .match(MsgWriteOK.class, this::onMsgWriteOK)
                .match(MsgReadRequest.class, this::onMsgReadRequest)
                .match(MsgWriteRequest.class, this::onMsgWriteRequest)
                .match(MsgHeartbeat.class, this::onMsgHeartbeat)
                .match(MsgElection.class, this::onMsgElection)
                .match(MsgElectionAck.class, this::onMsgElectionAck)
                .match(MsgCoordinator.class, this::onMsgCoordinator)
                .match(MsgSynchronization.class, this::onMsgSynchronization).build();
    }
}
