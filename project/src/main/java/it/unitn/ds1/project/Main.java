package it.unitn.ds1.project;

import java.io.IOException;
import java.util.ArrayList;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;

public class Main {
    final static int N_REPLICAS = 3;
    final static int N_CLIENTS = 1;

    public static void main(String[] args) {
        // Create an actor system named "ds1-project"
        final ActorSystem system = ActorSystem.create("ds1-project");

        // Create the replicas
        final ArrayList<ActorRef> replicas = new ArrayList<ActorRef>();
        for (int i = 0; i < N_REPLICAS; i++) {
            replicas.add(system.actorOf(Replica.props(i), "replica" + i));
        }

        final MsgReplicasInit msgInit = new MsgReplicasInit(replicas.toArray(new ActorRef[0]));
        for (int i = 0; i < N_REPLICAS; i++) {
            replicas.get(i).tell(msgInit, ActorRef.noSender());
        }

        // Create the clients
        for (int i = 0; i < N_CLIENTS; i++) {
            system.actorOf(Client.props(replicas.toArray(new ActorRef[0])), "client" + i);
        }

        System.out.println(">>> Press ENTER to exit <<<");
        try {
            System.in.read();
        }
        catch (IOException ioe) {}
        finally {
            system.terminate();
        }
    }
}
