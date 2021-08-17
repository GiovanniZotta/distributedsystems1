package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import it.unitn.ds1.actors.Checker;
import it.unitn.ds1.actors.Client;
import it.unitn.ds1.actors.Coordinator;
import it.unitn.ds1.actors.Server;
import it.unitn.ds1.messages.Message;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class Main {
    public final static int N_CLIENTS = 5;
    public final static int N_COORDINATORS = 3;
    public final static int N_SERVER = 5;
    public final static int MAX_KEY = N_SERVER * Server.DB_SIZE - 1;
    public final static Boolean CLIENT_DEBUG_BEGIN_TXN = false;
    public final static Boolean CLIENT_DEBUG_END_TXN = false;
    public final static Boolean CLIENT_DEBUG_READ_TXN = false;
    public final static Boolean CLIENT_DEBUG_WRITE_TXN = false;
    public final static Boolean CLIENT_DEBUG_READ_RESULT = false;
    public final static Boolean CLIENT_DEBUG_COMMIT_OK = false;
    public final static Boolean CLIENT_DEBUG_COMMIT_KO = false;
    public final static Boolean SERVER_DEBUG_SEND_VOTE = false;
    public final static Boolean SERVER_DEBUG_DECIDED = false;
    public final static Boolean COORD_DEBUG_DECISION = false;
    public final static Boolean NODE_DEBUG_STARTING_SIZE = false;
    public final static Boolean NODE_DEBUG_CRASH = false;
    public final static Boolean DEBUG_MULT_CRASH_ZERO = false;
    public final static Boolean DEBUG_MULT_CRASH_RANDOM = false;
    public final static Boolean DEBUG_MULT_CRASH_ALL = false;

    /*-- Timeout debug ---------------------------------------------------------*/
    public static final Boolean CLIENT_DEBUG_TIMEOUT_TXN_OPERATION = false;
    public static final Boolean CLIENT_DEBUG_TIMEOUT_TXN_ACCEPT = false;

    public final static int MAX_RECOVERY_TIME = 5000;      // timeout for the votes, ms
    public final static int TIMEOUT = 500;  // timeout for the decision, ms

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
            coordinators.add(system.actorOf(Coordinator.props(i, new HashSet<>()), "coordinator" + i));
        System.out.println("Coordinators created");

        // Create the servers
        List<ActorRef> servers = new ArrayList<>();
        for (int i = 0; i < N_SERVER; i++)
            servers.add(system.actorOf(Server.props(i, new HashSet<>()), "server" + i));
        System.out.println("Servers created");

        // Create the checker
        ActorRef checker = system.actorOf(Checker.props(), "checker");


        // Send start messages to the clients
        Message.CheckerMsg checkerMsg = new Message.CheckerMsg(checker);
        Message.WelcomeMsg startClients = new Message.WelcomeMsg(MAX_KEY, coordinators);
        for (ActorRef peer : clients) {
            peer.tell(startClients, null);
        }

        // Send start messages to the group
        Message.WelcomeMsg startOthers = new Message.WelcomeMsg(MAX_KEY, servers);
        for (ActorRef peer : coordinators) {
            peer.tell(checkerMsg, null);
            peer.tell(startOthers, null);
        }

        // Send start messages to the servers
        for (ActorRef peer : servers) {
            peer.tell(checkerMsg, null);
            peer.tell(startOthers, null);
        }
        checker.tell(new Message.CheckerWelcomeMsg(MAX_KEY, servers, coordinators), null);

        try {
            System.out.println(">>> Press ENTER to exit <<<");
            System.in.read();
        } catch (IOException ignored) {
        }

        Message.StopMsg stopMsg = new Message.StopMsg();
        for (ActorRef peer : clients) {
            peer.tell(stopMsg, null);
        }

        checker.tell(new Message.CheckCorrectness(), null);
        system.terminate();
    }
}
