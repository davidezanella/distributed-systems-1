package it.unitn.ds1.project;

import akka.actor.Props;
import akka.actor.AbstractActor;

public class Replica extends AbstractActor {
    private String v = "";

    // Some stuff required by Akka to create actors of this type
    static public Props props() {
        return Props.create(Replica.class, () -> new Replica());
    }

    private void onMsgReadValue(MsgReadValue m) {
        System.out.println("[" +
                getSelf().path().name() +      // the name of the current actor
                "] received a message from " +
                getSender().path().name() +    // the name of the sender actor
                ": read value"
        );

        //TODO: return the value v
    }

    private void onMsgWriteValue(MsgWriteValue m) {
        System.out.println("[" +
                getSelf().path().name() +      // the name of the current actor
                "] received a message from " +
                getSender().path().name() +    // the name of the sender actor
                ": write value" + m.newValue
        );

        //TODO: Update protocol
    }

    // Here we define the mapping between the received message types
    // and our actor methods
    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(MsgReadValue.class, this::onMsgReadValue)
                .match(MsgWriteValue.class, this::onMsgWriteValue).build();
    }
}
