package it.unitn.ds1.project;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.event.DiagnosticLoggingAdapter;
import akka.event.Logging;
import scala.concurrent.duration.Duration;

import javax.swing.Timer;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;

public class Replica extends AbstractActor {
    DiagnosticLoggingAdapter log = Logging.getLogger(this);

    final private Integer TIMEOUT = 10000; // Timeout value in milliseconds
    final private Integer MAX_RESP_DELAY = 0; // Maximum delay in seconds while sending a message

    private String v = "init";
    private boolean crashed = false;
    private Integer coordinatorIdx = 9;
    private Integer sequenceNumber = 0;
    private Integer epochNumber = 0;
    final private Integer id;
    private ActorRef[] replicas;

    private final ArrayDeque<MsgWriteRequest> pendingWriteRequestsWhileElection = new ArrayDeque<>();

    private final HashMap<UpdateKey, Integer> AckReceived = new HashMap<>(); // Used in the UPDATE phase
    private final HashMap<UpdateKey, String> pendingUpdates = new HashMap<>(); // waiting for quorum
    private final ArrayList<MsgWriteOK> updatesHistory = new ArrayList<>() {
        {
            add(new MsgWriteOK("init", new UpdateKey(-1, -1)));
        }
    };

    private final HashMap<String, MsgWriteRequest> pendingWriteRequestMsg = new HashMap<>(); // used to avoid loosing messages

    private final HashMap<UpdateKey, Timer> timersWriteOk = new HashMap<>();
    private final HashMap<String, Timer> timersUpdateRequests = new HashMap<>();
    private Timer timerCoordinatorHeartbeat;
    private Timer timerHeartbeat;
    private Timer timerElection;

    private boolean inElection = false;

    private final PriorityBlockingQueue<MsgWriteOK> writeOkQueue = new PriorityBlockingQueue<>();

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
            startCoordinatorElection();
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

        pendingWriteRequestsWhileElection.add(m);

        log.info("received " + m + " from " + getSender().path().name() + " - value: " + m.newValue);

        if (this.inElection) {
            return;
        }

        processPendingWriteRequests();
    }

    private void processPendingWriteRequests() {
        while (!pendingWriteRequestsWhileElection.isEmpty()) {
            MsgWriteRequest m = pendingWriteRequestsWhileElection.pollFirst();
            if (this.id.equals(this.coordinatorIdx)) {
                this.sequenceNumber++;

                UpdateKey key = new UpdateKey(this.epochNumber, this.sequenceNumber);
                this.AckReceived.put(key, 0);
                this.pendingUpdates.put(key, m.newValue);
                final MsgUpdate update = new MsgUpdate(m.newValue, key, m.requestId);

                // Send a broadcast to all the replicas
                broadcastToReplicas(update);
            } else {
                String requestId = Utils.generateRandomString();
                MsgWriteRequest req = new MsgWriteRequest(m.newValue, requestId);
                // forward the request to the coordinator
                boolean sent = sendOneMessage(replicas[coordinatorIdx], req);

                if (sent) {
                    this.pendingWriteRequestMsg.put(requestId, m);
                    Timer timerUpdate = new Timer(this.TIMEOUT, new actionUpdateTimeoutExceeded(requestId));
                    timerUpdate.setRepeats(false);
                    this.timersUpdateRequests.put(requestId, timerUpdate);
                    timerUpdate.start();
                }
            }
        }
    }

    private void onMsgUpdate(MsgUpdate m) {
        if (this.crashed)
            return;

        log.info("received " + m + " from " + getSender().path().name() + " " + m.key.toString() + " - value: " + m.value);

        if (this.timersUpdateRequests.containsKey(m.requestId)) {
            this.timersUpdateRequests.get(m.requestId).stop();
            this.timersUpdateRequests.remove(m.requestId);
        }

        // respond to the coordinator with an ACK
        MsgAck ack = new MsgAck(m.key);
        boolean sent = sendOneMessage(getSender(), ack);

        if (sent) {
            Timer timerWriteOk = new Timer(this.TIMEOUT, actionWriteOKTimeoutExceeded);
            timerWriteOk.setRepeats(false);
            this.timersWriteOk.put(m.key, timerWriteOk);
            timerWriteOk.start();
        }
    }

    private void onMsgAck(MsgAck m) throws Exception {
        if (this.crashed)
            return;

        log.info("received " + m + " from " + getSender().path().name() + " " + m.key.toString());

        if (!this.id.equals(this.coordinatorIdx)) {
            throw new Exception("Not coordinator replica receives ACK message!");
        }

        if (this.AckReceived.containsKey(m.key)) {
            int num = this.AckReceived.get(m.key) + 1;
            this.AckReceived.put(m.key, num);
            if (num >= Math.floor(this.replicas.length / 2.0) + 1) {
                final MsgWriteOK okMsg = new MsgWriteOK(this.pendingUpdates.get(m.key), m.key);
                broadcastToReplicas(okMsg);
                this.AckReceived.remove(m.key);
            }
        }
    }

    private void onMsgWriteOK(MsgWriteOK m) {
        if (this.crashed)
            return;

        log.info("received " + m + " from " + getSender().path().name() + " " + m.key.toString() + " - value: " + m.value);

        if (this.timersWriteOk.containsKey(m.key)) {
            this.timersWriteOk.get(m.key).stop();
            this.timersWriteOk.remove(m.key);
        }

        writeOkQueue.add(m);

        for (MsgWriteOK msg : writeOkQueue) {
            MsgWriteOK lastApplied = this.updatesHistory.get(this.updatesHistory.size() - 1);
            if (msg.key.epoch > lastApplied.key.epoch ||
                    (msg.key.epoch.equals(lastApplied.key.epoch) && msg.key.sequence.equals(lastApplied.key.sequence + 1))) {
                // store in the history the write
                this.updatesHistory.add(msg);

                log.info("applied " + msg.key.toString() + " - value: " + msg.value);

                writeOkQueue.remove(msg);
            }
        }

        MsgWriteOK lastApplied = this.updatesHistory.get(this.updatesHistory.size() - 1);
        this.v = lastApplied.value;


        if (this.id == 9)
            this.crashed = true;

    }

    private void onMsgHeartbeat(MsgHeartbeat m) {
        /*
        Every time it receives a heartbeat from the coordinator, it stops the previous timer and initiates a new one.
        */
        if (this.crashed)
            return;

        if (m != null)
            log.info("received " + m + " from " + getSender().path().name());

        if (this.timerHeartbeat != null)
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

    void startCoordinatorElection() {
        if (crashed)
            return;

        if (this.inElection) // an election is already running
            return;

        this.inElection = true;
        MsgElection election = new MsgElection();
        MsgWriteOK lastValue = updatesHistory.get(updatesHistory.size() - 1);
        election.nodesHistory.put(id, lastValue);
        election.seen.put(id, false);
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
        if (this.crashed)
            return;

        /*
        if (this.id == 4){
            this.crashed = true;
            return;
        }
         */

        this.inElection = true;

        log.info("received " + m + " from " + getSender().path().name());

        sendOneMessage(getSender(), new MsgElectionAck());

        nextReplicaTry = 0;
        ActorRef nextReplica = getNextReplica(nextReplicaTry);

        // I'm already in the message
        if (m.nodesHistory.containsKey(id)) {
            ArrayList<Integer> ids = new ArrayList<>(m.nodesHistory.keySet());
            ids.sort(new Comparator<Integer>() {
                @Override
                public int compare(Integer id1, Integer id2) {
                    int res = 0;
                    MsgWriteOK ls1 = m.nodesHistory.get(id1);
                    MsgWriteOK ls2 = m.nodesHistory.get(id2);
                    res = ls1.key.epoch.compareTo(ls2.key.epoch) * -1;
                    if (res == 0) {
                        res = ls1.key.sequence.compareTo(ls2.key.sequence) * -1;
                    }
                    if (res == 0) {
                        res = id1.compareTo(id2) * -1;
                    }
                    return res;
                }
            });

            Integer newCoord = ids.get(0);

            if (!newCoord.equals(coordinatorIdx)) {
                if (newCoord.equals(id)) {
                    // set up the new coordinator

                    if (this.updatesHistory.size() > 0) {
                        this.epochNumber = this.updatesHistory.get(this.updatesHistory.size() - 1).key.epoch + 1;
                        this.sequenceNumber = 0;
                    }

                    if (this.timerHeartbeat != null)
                        this.timerHeartbeat.stop(); //not needed since now it's the coordinator

                    this.timerCoordinatorHeartbeat = new Timer(this.TIMEOUT / 4, actionSendHeartbeat);
                    this.timerCoordinatorHeartbeat.setRepeats(true);
                    this.timerCoordinatorHeartbeat.start();

                    // Send SYNCHRONIZATION message and sync replicas
                    MsgSynchronization sync = new MsgSynchronization(this.id);

                    for (int repId : m.nodesHistory.keySet()) {
                        for (MsgWriteOK write : this.updatesHistory) {
                            if (write.key.epoch.equals(this.epochNumber - 1)
                                    && write.key.sequence > m.nodesHistory.get(repId).key.sequence) {
                                if (!sync.missingUpdates.contains(write))
                                    sync.missingUpdates.add(write);
                            }
                        }
                    }

                    broadcastToReplicas(sync);
                } else {
                    // forward if I'm not the new coordinator
                    if (m.seen.get(id).equals(false)) {
                        // at most one cycle done
                        m.seen.put(id, true);
                        sendOneMessage(nextReplica, m);
                    } else {
                        // more than one cycle done, restart the election cause the best candidate is crashed
                        startCoordinatorElection();
                    }
                }
            }
        } else {
            MsgWriteOK lastValue = updatesHistory.get(updatesHistory.size() - 1);
            m.nodesHistory.put(id, lastValue);
            m.seen.put(id, false);
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
        if (this.timerElection != null && this.timerElection.isRunning())
            this.timerElection.stop();

        if (this.crashed)
            return;

        log.info("received " + m + " from " + getSender().path().name());
    }

    private void onMsgSynchronization(MsgSynchronization m) {
        if (this.crashed)
            return;

        log.info("received " + m + " from " + getSender().path().name() + " - coordId: " + m.id);

        coordinatorIdx = m.id;

        for (MsgWriteOK write : m.missingUpdates) {
            if (!this.updatesHistory.contains(write)) {
                this.updatesHistory.add(write);
            }
        }

        this.inElection = false;
        processPendingWriteRequests();

        onMsgHeartbeat(null);
    }

    ActionListener actionSendHeartbeat = new ActionListener() {
        public void actionPerformed(ActionEvent actionEvent) {
            if (crashed)
                return;

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
                .match(MsgSynchronization.class, this::onMsgSynchronization).build();
    }


    ActionListener actionWriteOKTimeoutExceeded = new ActionListener() {
        public void actionPerformed(ActionEvent actionEvent) {
            if (crashed)
                return;

            log.info("timeout WriteOK");

            startCoordinatorElection();
        }
    };

    private class actionUpdateTimeoutExceeded implements ActionListener {
        private String requestId;

        public actionUpdateTimeoutExceeded(String requestId) {
            this.requestId = requestId;
        }

        public void actionPerformed(ActionEvent e) {
            if (crashed)
                return;

            log.info("timeout Update, adding message to queue");

            MsgWriteRequest m = pendingWriteRequestMsg.get(requestId);
            pendingWriteRequestsWhileElection.add(m);

            startCoordinatorElection();
        }
    }

    ActionListener actionHeartbeatTimeoutExceeded = new ActionListener() {
        public void actionPerformed(ActionEvent actionEvent) {
            if (crashed)
                return;

            log.info("timeout Heartbeat");

            startCoordinatorElection();
        }
    };

    ActionListener actionElectionTimeout = new ActionListener() {
        public void actionPerformed(ActionEvent actionEvent) {
            if (crashed)
                return;

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
}
