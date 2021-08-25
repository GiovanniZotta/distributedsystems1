package it.unitn.ds1.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.ds1.messages.Message;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


public class Checker extends AbstractActor {
    // number of coordinators and servers that have answered
    private Integer counterCoordinators;
    private Integer counterServers;
    private Set<ActorRef> coordinators;
    private Set<ActorRef> servers;
    // information collected from coordinators and servers that have answered
    private Integer partialSum;
    private final Map<ActorRef, Node.CrashPhaseMap> numServerCrashes;
    private final Map<ActorRef, Node.CrashPhaseMap> numCoordinatorCrashes;

    public Checker() {
        this.counterCoordinators = 0;
        this.counterServers = 0;
        this.partialSum = 0;
        this.numServerCrashes = new HashMap<>();
        this.numCoordinatorCrashes = new HashMap<>();
    }

    static public Props props() {
        return Props.create(Checker.class, () -> new Checker());
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Message.CheckerWelcomeMsg.class, this::onCheckerWelcomeMsg)
                .match(Message.CheckCorrectness.class, this::onCheckCorrectness)
                .match(Message.CheckCorrectnessResponse.class, this::onCheckCorrectnessResponse)
                .build();
    }


    public void onCheckerWelcomeMsg(Message.CheckerWelcomeMsg msg) {                   /* Start */
        servers = new HashSet<>(msg.servers);
        coordinators = new HashSet<>(msg.coordinators);
    }

    public void onCheckCorrectness(Message.CheckCorrectness msg) {

        System.out.println("CHECKING CORRECTNESS");
        for (ActorRef coordinator : coordinators) {
            coordinator.tell(new Message.CheckCorrectness(), getSelf());
        }

        for (ActorRef server : servers) {
            server.tell(new Message.CheckCorrectness(), getSelf());
        }

    }

    private void printCrashes(Map<ActorRef, Node.CrashPhaseMap> numCoordinatorCrashes) {
        System.out.println(Node.CrashPhaseMap.sumMaps(numCoordinatorCrashes.values()));
    }

    private void manageServer(Message.CheckCorrectnessResponse msg) throws InterruptedException {
        // manage info message from the server
        partialSum += msg.sumOfKeys;
        counterServers++;
        numServerCrashes.put(getSender(), msg.numCrashes);
        // when every server answered, print a summary of the information
        if (counterServers == servers.size()) {
            System.out.println("/---- SERVER CRASHES ----/");
            printCrashes(numServerCrashes);
            System.out.println("##### CORRECTNESS CHECK #####");
            Integer correctSum = servers.size() * (Server.DB_SIZE * Server.DEFAULT_VALUE);
            System.out.println("CORRECT SUM: " + correctSum);
            System.out.println("ACTUAL SUM: " + partialSum);
            assert (partialSum == correctSum);
            System.out.println("##### CORRECTNESS CHECK #####");
        }
    }

    private void manageCoordinator(Message.CheckCorrectnessResponse msg) throws InterruptedException {
        // manage info message from the coordinator
        counterCoordinators++;
        numCoordinatorCrashes.put(getSender(), msg.numCrashes);
        // when every coordinator answered, print a summary of the information
        if (counterCoordinators == coordinators.size()) {
            System.out.println("/---- COORDINATOR CRASHES ----/");
            printCrashes(numCoordinatorCrashes);
        }
    }

    public void onCheckCorrectnessResponse(Message.CheckCorrectnessResponse msg) throws InterruptedException {
        if (coordinators.contains(getSender())) {
            manageCoordinator(msg);
        } else if (servers.contains(getSender())) {
            manageServer(msg);
        }
    }

}