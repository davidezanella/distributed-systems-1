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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;

public class Replica extends AbstractActor {
    DiagnosticLoggingAdapter log = Logging.getLogger(this);

    final private Integer TIMEOUT = 10000; // Timeout value in milliseconds
    final private Integer MAX_RESP_DELAY = 0; // Maximum delay in seconds while sending a message

    // Set with the IDs of the replicas to schedule their crash in a specific phase of the protocol
    final private HashSet<Integer> CRASH_COORD_SENDING_UPDATE = new HashSet<>() {};
    final private HashSet<Integer> CRASH_ON_UPDATE = new HashSet<>() { { add(8); } };
    final private HashSet<Integer> CRASH_COORD_ON_ACK = new HashSet<>() {};
    final private HashSet<Integer> CRASH_AFTER_WRITEOK = new HashSet<>() {};
    final private HashSet<Integer> CRASH_ON_WRITEOK = new HashSet<>() {};
    final private HashSet<Integer> CRASH_ON_MSGELECTION = new HashSet<>() {};
    final private HashSet<Integer> CRASH_SENDING_SYNC = new HashSet<>() { { add(9); } };

    private String v = "init"; // value that the replica write and read
    private boolean crashed = false; // states if the replica is crashed or not
    private boolean inElection = false; // states if the replica is in an election phase
    private Integer coordinatorId; // ID of the coordinator
    private Integer sequenceNumber = 0; // current sequence number if the replica is the coordinator
    private Integer epochNumber = -1; // current epoch number if the replica if the coordinator
    final private Integer id; // ID of the current replica
    private ActorRef[] replicas; // set of all the replicas in the system, crashed and not

    private Integer nextReplicaTry = 0; // distance from the next neighbour replica
    private Serializable messageToSend; // store the message to send when next replica is crashed during an election

    // used to store the MsgWriteRequests while an election is going on
    private final ArrayDeque<MsgWriteRequest> pendingWriteRequestsWhileElection = new ArrayDeque<>();
    // used to store the MsgUpdates to avoid loosing them when coordinator crashes on ACK reception
    private final ConcurrentHashMap<UpdateKey, MsgUpdate> pendingUpdateRequests = new ConcurrentHashMap<>();

    // used to count the ACKs received in the UPDATE phase
    private final ConcurrentHashMap<UpdateKey, Integer> AckReceived = new ConcurrentHashMap<>();
    // used to keep the new value while waiting for the quorum
    private final ConcurrentHashMap<UpdateKey, String> pendingUpdates = new ConcurrentHashMap<>();
    // history of the updates applied by the replica
    private final ArrayList<MsgWriteOK> updatesHistory = new ArrayList<>() {
        {
            // initial value
            add(new MsgWriteOK("init", new UpdateKey(-1, -1)));
        }
    };

    // used to keep MsgWriteRequests between the Broadcast and Update phases
    private final ConcurrentHashMap<String, MsgWriteRequest> pendingWriteRequestMsg = new ConcurrentHashMap<>();
    // used to store MsgWriteOK and apply them only in order
    private final PriorityBlockingQueue<MsgWriteOK> writeOkQueue = new PriorityBlockingQueue<>();

    // map of the timers for catching timeouts while going from the Update phase to the WriteOk one
    private final ConcurrentHashMap<UpdateKey, Timer> timersWriteOk = new ConcurrentHashMap<>();
    // map of the timers for catching timeouts while going from the Broadcast phase to the Update one
    private final ConcurrentHashMap<String, Timer> timersUpdateRequests = new ConcurrentHashMap<>();
    // timer to send Heartbeats from the coordinator to all the replicas
    private Timer timerCoordinatorHeartbeat;
    // timer to manage the timeouts while waiting for Heartbeats from the coordinator
    private Timer timerHeartbeat;
    // timer that is triggered when, during an election, a replica doesn't answer with an election ACK message
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

    // Special message used to set the replicas array and start an election to decide the coordinator
    private void onMsgReplicasInit(MsgReplicasInit m) {
        this.replicas = m.replicas;
        if (this.coordinatorId == null) {
            // init coordinator election
            startCoordinatorElection(false);
        }
    }

    // Handles the ReadRequests coming from a Client, returns the value of v
    private void onMsgReadRequest(MsgReadRequest m) {
        if (this.crashed)
            return;

        log.info("received " + m + " from " + getSender().path().name());

        // send to the client a message with the value of v
        sendOneMessage(getSender(), new MsgReadResponse(v));
    }

    // Handles the WriteRequests, it adds the message to a queue and, if not in election process all the WriteRequests of the queue
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

    /**
     * Process the MsgWriteRequests present in the queue.
     * If this replica is the coordinator, it creates a new Update request to send to all the replicas.
     * Otherwise, the replica attach to the Write request a special requestId to recognize the answer later on, and
     * forwards this new request to the coordinator. A timer is created to be sure that the Write request will not be lost.
     */
    private void processPendingWriteRequests() {
        while (!pendingWriteRequestsWhileElection.isEmpty()) {
            MsgWriteRequest m = pendingWriteRequestsWhileElection.pollFirst();
            if (this.id.equals(this.coordinatorId)) { // I'm the coordinator
                this.sequenceNumber++;

                UpdateKey key = new UpdateKey(this.epochNumber, this.sequenceNumber);
                this.AckReceived.put(key, 0);
                this.pendingUpdates.put(key, m.newValue); // to store the proposed value
                final MsgUpdate update = new MsgUpdate(m.newValue, key, m.requestId);

                if (CRASH_COORD_SENDING_UPDATE.contains(this.id)) { // to simulate a crash
                    getSelf().tell(new MsgCrash(), ActorRef.noSender());
                    return;
                }

                // Send a broadcast to all the replicas
                broadcastToReplicas(update);
            } else {
                String requestId = Utils.generateRandomString();
                MsgWriteRequest req = new MsgWriteRequest(m.newValue, requestId);
                // forward the request to the coordinator
                sendOneMessage(replicas[coordinatorId], req);

                this.pendingWriteRequestMsg.put(requestId, m);
                Timer timerUpdate = new Timer(this.TIMEOUT, new actionUpdateTimeoutExceeded(m));
                timerUpdate.setRepeats(false);
                this.timersUpdateRequests.put(requestId, timerUpdate);
                timerUpdate.start();
            }
        }
    }

    // Handles the delivery of an Update message.
    // Send back to the coordinator an ACK message and a timer is created to avoid loosing the update if the coordinator crashes.
    private void onMsgUpdate(MsgUpdate m) {
        if (this.crashed)
            return;

        log.info("received " + m + " from " + getSender().path().name() + " " + m.key.toString() + " - value: " + m.value);

        pendingUpdateRequests.put(m.key, m); // to avoid loosing the request if the coordinator crashes

        if (CRASH_ON_UPDATE.contains(this.id)) { // to simulate a crash
            getSelf().tell(new MsgCrash(), ActorRef.noSender());
            return;
        }

        MsgWriteRequest mReq = null;
        if (m.requestId != null) { // I created the MsgWriteRequest linked to this update
            if (pendingWriteRequestMsg.containsKey(m.requestId))
                mReq = pendingWriteRequestMsg.remove(m.requestId);

            if (this.timersUpdateRequests.containsKey(m.requestId)) {
                this.timersUpdateRequests.get(m.requestId).stop();
                this.timersUpdateRequests.remove(m.requestId);
            }
        }

        // respond to the coordinator with an ACK
        MsgAck ack = new MsgAck(m.key);
        boolean sent = sendOneMessage(getSender(), ack);

        if (sent) {
            Timer timerWriteOk = new Timer(this.TIMEOUT, new actionWriteOKTimeoutExceeded(mReq));
            timerWriteOk.setRepeats(false);
            this.timersWriteOk.put(m.key, timerWriteOk);
            timerWriteOk.start();
        }
    }

    // Handles the Ack messages coming from the replicas after an Update message.
    // Counts the ACK received and in case of quorum, sends the WrietOK message to all the replicas.
    private void onMsgAck(MsgAck m) throws Exception {
        if (this.crashed)
            return;

        log.info("received " + m + " from " + getSender().path().name() + " " + m.key.toString());

        if (CRASH_COORD_ON_ACK.contains(this.id)) { // to simulate a crash
            getSelf().tell(new MsgCrash(), ActorRef.noSender());
            return;
        }

        if (!this.id.equals(this.coordinatorId)) {
            throw new Exception("Not coordinator replica received an ACK message!");
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

    // Handles the message WriteOk coming from the coordinator.
    // Stops the timer set in the Update phase and applies only the correct updates
    private void onMsgWriteOK(MsgWriteOK m) {
        if (this.crashed)
            return;

        log.info("received " + m + " from " + getSender().path().name() + " " + m.key.toString() + " - value: " + m.value);

        pendingUpdateRequests.remove(m.key); // remove from the pending updates since now it's not pending anymore

        if (CRASH_ON_WRITEOK.contains(this.id)) { // to simulate a crash
            getSelf().tell(new MsgCrash(), ActorRef.noSender());
            return;
        }

        if (this.timersWriteOk.containsKey(m.key)) { // stop the timer that waits for the WriteOK linked the Update message
            this.timersWriteOk.get(m.key).stop();
            this.timersWriteOk.remove(m.key);
        }

        writeOkQueue.add(m); // add to the list of updates to apply

        for (MsgWriteOK msg : writeOkQueue) {
            MsgWriteOK lastApplied = this.updatesHistory.get(this.updatesHistory.size() - 1);
            // check if msg is the next update following the last applied (sequence number increased by 1)
            if (msg.key.epoch > lastApplied.key.epoch ||
                    (msg.key.epoch.equals(lastApplied.key.epoch) && msg.key.sequence.equals(lastApplied.key.sequence + 1))) {
                // store in the history the write
                this.updatesHistory.add(msg);

                log.info("applied " + msg.key.toString() + " - value: " + msg.value);

                writeOkQueue.remove(msg);
            }
        }

        MsgWriteOK lastApplied = this.updatesHistory.get(this.updatesHistory.size() - 1);
        this.v = lastApplied.value; // assign to v only the last update's value

        if (CRASH_AFTER_WRITEOK.contains(this.id)) { // to simulate a crash
            getSelf().tell(new MsgCrash(), ActorRef.noSender());
            return;
        }
    }

    // Handles heartbeats coming from the coordinator
    // It stops the previous Heartbeat timer and initiates a new one.
    private void onMsgHeartbeat(MsgHeartbeat m) {
        if (this.crashed)
            return;

        this.inElection = false; // for sure we are not in election, cause heartbeats come only from a coordinator

        if (m != null)
            log.info("received " + m + " from " + getSender().path().name());

        if (this.timerHeartbeat != null)
            this.timerHeartbeat.stop();

        this.timerHeartbeat = new Timer(this.TIMEOUT, actionHeartbeatTimeoutExceeded);
        this.timerHeartbeat.setRepeats(false);
        this.timerHeartbeat.start();
    }

    // Returns the ID of the next, probably not crashed, replica of the ring
    private ActorRef getNextReplica(Integer idx) {
        return replicas[(id + idx + 1) % replicas.length];
    }

    // Sends a message to all the replicas
    private void broadcastToReplicas(Serializable message) {
        for (ActorRef replica : this.replicas) {
            boolean sent = sendOneMessage(replica, message);
            if (!sent)
                return;
        }
    }

    // If the replica is not crashed, it sends a message to an actor with a random delay
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

    /**
     * Starts the election of a coordinator sending an Election message to the next replica of the ring.
     * A timer is created to manage the case were the next replica is crashed.
     * @param forceStart start the election even if we already are in an election phase
     */
    void startCoordinatorElection(Boolean forceStart) {
        if (crashed)
            return;

        if (this.inElection && !forceStart) // an election is already running and we don't want to start a new one
            return;

        this.inElection = true;
        MsgElection election = new MsgElection();
        MsgWriteOK lastValue = updatesHistory.get(updatesHistory.size() - 1);
        election.nodesHistory.put(id, lastValue);
        election.seen.put(id, false);
        ActorRef nextReplica = getNextReplica(nextReplicaTry);
        sendOneMessage(nextReplica, election);

        messageToSend = election;

        this.timerElection = new Timer(this.TIMEOUT, actionElectionTimeout);
        this.timerElection.setRepeats(false);
        this.timerElection.start();
    }

    // Handles the Election messages, sending an ElectionACK to the previous replica in the ring and forwarding
    // the Election message or announcing the new leadership with a Synchronization message.
    // It manages the case where the best candidate is crashed during the election phase forcing the restart of the election.
    private void onMsgElection(MsgElection m) {
        if (this.crashed)
            return;

        if (CRASH_ON_MSGELECTION.contains(this.id)) { // to simulate a crash
            getSelf().tell(new MsgCrash(), ActorRef.noSender());
            return;
        }

        this.inElection = true;

        log.info("received " + m + " from " + getSender().path().name());

        sendOneMessage(getSender(), new MsgElectionAck());

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

            if (!newCoord.equals(coordinatorId)) {
                if (newCoord.equals(id)) {
                    // set up the new coordinator
                    coordinatorId = newCoord;
                    this.epochNumber += 1;
                    this.sequenceNumber = 0;

                    if (this.timerHeartbeat != null)
                        this.timerHeartbeat.stop(); //not needed since now I'm the coordinator

                    // Needed to start sending Heartbeats
                    this.timerCoordinatorHeartbeat = new Timer(this.TIMEOUT / 4, actionSendHeartbeat);
                    this.timerCoordinatorHeartbeat.setRepeats(true);
                    this.timerCoordinatorHeartbeat.start();

                    // Add pending updates as new MsgWriteRequests
                    for (UpdateKey key : pendingUpdateRequests.keySet()) {
                        MsgUpdate updReq = pendingUpdateRequests.remove(key);
                        pendingWriteRequestsWhileElection.add(new MsgWriteRequest(updReq.value, updReq.requestId));
                    }

                    // Send SYNCHRONIZATION message and sync replicas
                    MsgSynchronization sync = new MsgSynchronization(this.id, this.epochNumber);

                    for (int repId : m.nodesHistory.keySet()) {
                        for (MsgWriteOK write : this.updatesHistory) {
                            if (write.key.epoch.equals(this.epochNumber - 1)
                                    && write.key.sequence > m.nodesHistory.get(repId).key.sequence) {
                                if (!sync.missingUpdates.contains(write))
                                    sync.missingUpdates.add(write);
                            }
                        }
                    }
                    if (CRASH_SENDING_SYNC.contains(this.id)) { // to simulate a crash
                        getSelf().tell(new MsgCrash(), ActorRef.noSender());
                        return;
                    }
                    broadcastToReplicas(sync);
                } else {
                    // forward the election message cause if I'm not the new coordinator
                    if (m.seen.get(id).equals(false)) { // at most one cycle done
                        m.seen.put(id, true);
                        sendOneMessage(nextReplica, m);

                        messageToSend = m;

                        if (this.timerElection != null && this.timerElection.isRunning())
                            this.timerElection.stop();
                        this.timerElection = new Timer(this.TIMEOUT, actionElectionTimeout);
                        this.timerElection.setRepeats(false);
                        this.timerElection.start();
                    } else {
                        // more than one cycle done, restart the election cause the best candidate is crashed
                        startCoordinatorElection(true);
                    }
                }
            }
        } else {
            // insert myself in the candidate list and forward the message
            MsgWriteOK lastValue = updatesHistory.get(updatesHistory.size() - 1);
            m.nodesHistory.put(id, lastValue);
            m.seen.put(id, false);
            sendOneMessage(nextReplica, m);

            messageToSend = m;

            // start a timer to be sure that the next replica of the ring is not crashed
            if (this.timerElection != null && this.timerElection.isRunning())
                this.timerElection.stop();
            this.timerElection = new Timer(this.TIMEOUT, actionElectionTimeout);
            this.timerElection.setRepeats(false);
            this.timerElection.start();
        }
    }

    // Handles the delivery of Election ACK messages.
    // With this kind of messages simply stop the election timer, cause the next replica is not crashed.
    private void onMsgElectionAck(MsgElectionAck m) {
        if (this.timerElection != null && this.timerElection.isRunning())
            this.timerElection.stop();

        if (this.crashed)
            return;

        log.info("received " + m + " from " + getSender().path().name());
    }

    // Handles the Synchronization messages.
    // Set the new coordinatorID and epoch number. Apply the missing updates.
    private void onMsgSynchronization(MsgSynchronization m) {
        if (this.crashed)
            return;

        log.info("received " + m + " from " + getSender().path().name() + " - coordId: " + m.id);

        coordinatorId = m.id;
        epochNumber = m.epoch;

        // delete pending updates, the coordinator will handle them
        pendingUpdateRequests.clear();

        for (MsgWriteOK write : m.missingUpdates) {
            if (!this.updatesHistory.contains(write)) {
                this.updatesHistory.add(write);
            }
        }

        this.inElection = false; // the coordinator just announced himself
        processPendingWriteRequests(); // election is over and we can process pending write requests

        onMsgHeartbeat(null); // start the Heartbeat timer
    }

    // Handles Crash messages. The replica is crashed.
    private void onMsgCrash(MsgCrash m) {
        log.info("CRASHED");

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

    // The coordinator uses this action to periodically send heartbeats
    ActionListener actionSendHeartbeat = new ActionListener() {
        public void actionPerformed(ActionEvent actionEvent) {
            if (crashed)
                return;

            broadcastToReplicas(new MsgHeartbeat());
        }
    };

    // No WriteOK received after seeing an Update message. Missed WriteRequests will be processed again.
    private class actionWriteOKTimeoutExceeded implements ActionListener {
        private MsgWriteRequest mReq;

        public actionWriteOKTimeoutExceeded(MsgWriteRequest mReq) {
            this.mReq = mReq;
        }

        public void actionPerformed(ActionEvent e) {
            if (crashed)
                return;

            if (this.mReq != null) { // I've forwarded the MsgWriteRequest to the coordinator
                log.info("timeout WriteOK, adding message to queue");

                pendingWriteRequestsWhileElection.add(this.mReq);
            } else { // I didn't manage the original MsgWriteRequest coming from the client
                log.info("timeout WriteOK");
            }

            startCoordinatorElection(false);
        }
    }

    // No Update messages received after sending a WriteRequest to the coordinator. Missed WriteRequests will be processed again.
    private class actionUpdateTimeoutExceeded implements ActionListener {
        private MsgWriteRequest mReq;

        public actionUpdateTimeoutExceeded(MsgWriteRequest mReq) {
            this.mReq = mReq;
        }

        public void actionPerformed(ActionEvent e) {
            if (crashed)
                return;

            log.info("timeout Update, adding message to queue");

            pendingWriteRequestsWhileElection.add(this.mReq);

            startCoordinatorElection(false);
        }
    }

    // No heartbeats received within a TIMEOUT interval. The coordinator is crashed.
    ActionListener actionHeartbeatTimeoutExceeded = new ActionListener() {
        public void actionPerformed(ActionEvent actionEvent) {
            if (crashed)
                return;

            log.info("timeout Heartbeat");

            startCoordinatorElection(false);
        }
    };

    // No Election ACKs received from the next replica of the ring within a TIMEOUT interval. Try with the successive replica.
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
