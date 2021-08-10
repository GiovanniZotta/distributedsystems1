package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import it.unitn.ds1.messages.CoordinatorServerMessages.Vote;
import it.unitn.ds1.messages.ClientCoordinatorMessages;
import it.unitn.ds1.messages.Message;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Main {
    final static int N_CLIENTS = 3;
    final static int N_COORDINATORS = 3;
    final static int N_SERVER = 3;
    final static int MAX_KEY = N_SERVER * Server.DB_SIZE - 1;

    final static int VOTE_TIMEOUT = 1000;      // timeout for the votes, ms
    final static int DECISION_TIMEOUT = 2000;  // timeout for the decision, ms

    // the votes that the participants will send (for testing)
    final static Vote[] predefinedVotes =
            new Vote[]{Vote.YES, Vote.YES, Vote.YES}; // as many as N_PARTICIPANTS



    /*-- Main ------------------------------------------------------------------*/
    public static void main(String[] args) {

        // Create the actor system
        final ActorSystem system = ActorSystem.create("concurrent2pc");

        // Create the clients
        List<ActorRef> clients = new ArrayList<>();
        for (int i = 0; i < N_CLIENTS; i++)
            clients.add(system.actorOf(Client.props(i), "client" + i));
        System.out.println("Clients created");

        // Create the coordinators
        List<ActorRef> coordinators = new ArrayList<>();
        for (int i = 0; i < N_COORDINATORS; i++)
            coordinators.add(system.actorOf(Coordinator.props(i), "coordinator" + i));
        System.out.println("Coordinators created");

        // Create the servers
        List<ActorRef> servers = new ArrayList<>();
        for (int i = 0; i < N_SERVER; i++)
            servers.add(system.actorOf(Server.props(i), "server" + i));
        System.out.println("Servers created");

        // Create the checker
        ActorRef checker = system.actorOf(Checker.props(N_SERVER), "checker");


        // Send start messages to the clients
        Message.WelcomeMsg startClients = new Message.WelcomeMsg(MAX_KEY, coordinators);
        for (ActorRef peer : clients) {
            peer.tell(startClients, null);
        }

        // Send start messages to the group
        Message.WelcomeMsg startOthers = new Message.WelcomeMsg(MAX_KEY, servers);
        for (ActorRef peer : coordinators) {
            peer.tell(startOthers, null);
        }

        // Send start messages to the servers
        for (ActorRef peer : servers) {
            peer.tell(startOthers, null);
        }
        checker.tell(startOthers, null);

        try {
            System.out.println(">>> Press ENTER to exit <<<");
            System.in.read();
        } catch (IOException ignored) {
        }

        checker.tell(new Message.CheckCorrectness(), null);
        system.terminate();
    }
}
