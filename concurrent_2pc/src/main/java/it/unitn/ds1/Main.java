package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import it.unitn.ds1.actors.*;
import it.unitn.ds1.messages.Message;
import scala.concurrent.duration.Duration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class Main {

    /*-- System parameters ---------------------------------------------------------*/
    public final static int N_CLIENTS = 5;
    public final static int N_COORDINATORS = 3;
    public final static int N_SERVER = 5;
    public final static int MAX_KEY = N_SERVER * Server.DB_SIZE - 1;

    /*-- Crash parameters ---------------------------------------------------------*/

    public final static int MIN_RECOVERY_TIME = 1; // minimum recovery time for nodes, ms
    public final static int MAX_RECOVERY_TIME = 5000; // maximum recovery time for nodes, ms
    public final static int COORD_TIMEOUT = 500;  // coordinator timeout, ms
    public final static int SERVER_TIMEOUT = 1000;  // server timeout, ms
    public final static int CLIENT_TIMEOUT = 1000;  // timeout for client, ms
    public static final double COORD_CRASH_PROBABILITY = 0.001;
    public static final double SERVER_CRASH_PROBABILITY = 0.001;
    public static final int MAX_NODE_DELAY = 5; // network delay
    public static final int CORRECTNESS_DELAY = 10000;

    /*-- Node debug ---------------------------------------------------------*/
    public final static Boolean NODE_DEBUG_STARTING_SIZE = false;
    public final static Boolean NODE_DEBUG_CRASH = true;

    /*-- Client debug ---------------------------------------------------------*/
    public final static Boolean CLIENT_DEBUG_BEGIN_TXN = false;
    public final static Boolean CLIENT_DEBUG_END_TXN = false;
    public final static Boolean CLIENT_DEBUG_READ_TXN = false;
    public final static Boolean CLIENT_DEBUG_WRITE_TXN = false;
    public final static Boolean CLIENT_DEBUG_READ_RESULT = false;
    public final static Boolean CLIENT_DEBUG_COMMIT_OK = true;
    public final static Boolean CLIENT_DEBUG_COMMIT_KO = false;
    public static final Boolean CLIENT_DEBUG_TIMEOUT_TXN_OPERATION = true;
    public static final Boolean CLIENT_DEBUG_TIMEOUT_TXN_ACCEPT = true;

    /*-- Server debug ---------------------------------------------------------*/
    public final static Boolean SERVER_DEBUG_SEND_VOTE = false;
    public final static Boolean SERVER_DEBUG_DECIDED = false;
    public static final Boolean SERVER_DEBUG_SET_TIMEOUT = false;
    public static final Boolean SERVER_DEBUG_UNSET_TIMEOUT = false;
    public static final Boolean SERVER_DEBUG_RECOVERY = true;
    public static final Boolean SERVER_DEBUG_READ = false;
    public static final Boolean SERVER_DEBUG_TIMEOUT = true;

    /*-- Coordinator debug ---------------------------------------------------------*/
    public static final Boolean COORD_DEBUG_RECOVERY = true;
    public static final Boolean COORD_DEBUG_UNSET_TIMEOUT = false;
    public final static Boolean COORD_DEBUG_DECISION = false;
    public static final Boolean COORD_DEBUG_BEGIN_VOTE = false;
    public static final Boolean COORD_DEBUG_SET_TIMEOUT = false;
    public static final Boolean COORD_DEBUG_BEGIN_TXN = false;
    public static final Boolean COORD_DEBUG_TIMEOUT = false;
    public static final Boolean COORD_DEBUG_RECEIVED_VOTE = false;
    public static final Boolean COORD_DEBUG_ALL_VOTED_YES = false;
    public static final Boolean COORD_DEBUG_READ = false;
    public static final Boolean COORD_DEBUG_READ_RESPONSE = false;

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

        /*-- Coordinator crash phases ---------------------------------------------------------*/
        Set<Node.CrashPhase> coordinatorCrashPhases = new HashSet<>();
        coordinatorCrashPhases.add(Coordinator.CrashBefore2PC.BEFORE_TXN_ACCEPT_MSG);
//        coordinatorCrashPhases.add(Coordinator.CrashBefore2PC.ON_CLIENT_MSG);
//        coordinatorCrashPhases.add(Coordinator.CrashBefore2PC.ON_SERVER_MSG);
        coordinatorCrashPhases.add(Coordinator.CrashDuring2PC.CrashDuringVote.ALL_MSG);
//        coordinatorCrashPhases.add(Coordinator.CrashDuring2PC.CrashDuringVote.RND_MSG);
        coordinatorCrashPhases.add(Coordinator.CrashDuring2PC.CrashDuringVote.ZERO_MSG);
        coordinatorCrashPhases.add(Coordinator.CrashDuring2PC.CrashDuringDecision.ALL_MSG);
//        coordinatorCrashPhases.add(Coordinator.CrashDuring2PC.CrashDuringDecision.RND_MSG);
        coordinatorCrashPhases.add(Coordinator.CrashDuring2PC.CrashDuringDecision.ZERO_MSG);

        for (int i = 0; i < N_COORDINATORS; i++)
            coordinators.add(system.actorOf(Coordinator.props(i, coordinatorCrashPhases), "coordinator" + i));
        System.out.println("Coordinators created");

        // Create the servers
        List<ActorRef> servers = new ArrayList<>();

        /*-- Server crash phases ---------------------------------------------------------*/
        Set<Node.CrashPhase> serverCrashPhases = new HashSet<>();
//        serverCrashPhases.add(Server.CrashBefore2PC.ON_COORD_MSG);
//        serverCrashPhases.add(Server.CrashDuring2PC.CrashDuringVote.NO_VOTE);
//        serverCrashPhases.add(Server.CrashDuring2PC.CrashDuringVote.AFTER_VOTE);
        serverCrashPhases.add(Server.CrashDuring2PC.CrashDuringTermination.ALL_REPLY);
//        serverCrashPhases.add(Server.CrashDuring2PC.CrashDuringTermination.RND_REPLY);
        serverCrashPhases.add(Server.CrashDuring2PC.CrashDuringTermination.NO_REPLY);

        for (int i = 0; i < N_SERVER; i++)
            servers.add(system.actorOf(Server.props(i, serverCrashPhases), "server" + i));
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

        system.scheduler().scheduleOnce(
                Duration.create(Main.CORRECTNESS_DELAY, TimeUnit.MILLISECONDS),
                checker,
                new Message.CheckCorrectness(),
                system.dispatcher(), ActorRef.noSender()
        );

        try {
            Thread.sleep(Main.CORRECTNESS_DELAY + (10*Main.MAX_NODE_DELAY) + 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        system.terminate();
    }
}
