package it.unitn.ds1.actors;

/*-- Common functionality for both Coordinator and Participants ------------*/

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import it.unitn.ds1.Main;
import it.unitn.ds1.transactions.Transaction;
import it.unitn.ds1.messages.CoordinatorServerMessage;
import it.unitn.ds1.messages.Message;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;

public abstract class Node extends AbstractActor {
    public interface CrashPhase {}
    public static class CrashPhaseMap extends HashMap<CrashPhase, Integer> {
        @Override
        public String toString() {
            String res = "";
            for (Entry<CrashPhase, Integer> entry : this.entrySet()){
                res += entry.getKey().toString() + ": " + entry.getValue() + "\n";
            }
            return res.substring(0, res.length() - 1);
        }

        public static CrashPhaseMap sumMaps(Collection<CrashPhaseMap> maps) {
            CrashPhaseMap res = new CrashPhaseMap();
            for (CrashPhaseMap map : maps)
                for (Entry<CrashPhase, Integer> entry : map.entrySet())
                    res.put(entry.getKey(), res.getOrDefault(entry.getKey(), 0) + entry.getValue());
            return res;
        }
    }

    protected static final double CRASH_PROBABILITY = 0.1;
    protected int id;                           // node ID
    protected List<ActorRef> servers;      // list of participant nodes
    protected ActorRef checker;
    protected final Map<Transaction, CoordinatorServerMessage.Decision> transaction2decision;
    protected final Random r;
    protected final Set<CrashPhase> crashPhases;
    protected final CrashPhaseMap numCrashes;

    public Node(int id, Set<CrashPhase> crashPhases) {
        super();
        this.id = id;
        this.crashPhases = crashPhases;
        transaction2decision = new HashMap<>();
        numCrashes = new CrashPhaseMap();
        r = new Random();
    }

    // abstract method to be implemented in extending classes
    protected abstract void onRecovery(CoordinatorServerMessage.Recovery msg);

    void setGroup(Message.WelcomeMsg sm) {
        servers = new ArrayList<>();
        for (ActorRef b : sm.group) {
            if (!b.equals(getSelf())) {

                // copying all participant refs except for self
                this.servers.add(b);
            }
        }
        if(Main.NODE_DEBUG_STARTING_SIZE)
            print("starting with " + sm.group.size() + " peer(s)");
    }

    // emulate a crash and a recovery in a given time
    void crash(int recoverIn, CrashPhase crashPhase) {
        getContext().become(crashed());
        numCrashes.put(crashPhase, numCrashes.getOrDefault(crashPhase, 0) + 1);
        if(Main.NODE_DEBUG_CRASH)
            print("CRASH!!!");

        // setting a timer to "recover"
        getContext().system().scheduler().scheduleOnce(
                Duration.create(recoverIn, TimeUnit.MILLISECONDS),
                getSelf(),
                new CoordinatorServerMessage.Recovery(), // message sent to myself
                getContext().system().dispatcher(), getSelf()
        );
    }

    Boolean maybeCrash(CrashPhase crashPhase) {
        if (crashPhases.contains(crashPhase) && r.nextDouble() < CRASH_PROBABILITY) {
            crash(1 + r.nextInt(Main.MAX_RECOVERY_TIME), crashPhase);
            return true;
        }
        return false;
    }

    // emulate a delay of d milliseconds
    void delay(int d) {
        try {
            Thread.sleep(d);
        } catch (Exception ignored) {
        }
    }

    void multicast(Serializable m, Collection<ActorRef> group) {
        for (ActorRef p : group)
            p.tell(m, getSelf());
    }

    // a multicast implementation that crashes after sending the first message
    abstract void multicastAndCrash(Serializable m, int recoverIn);

    // schedule a Timeout message in specified time
    void setTimeout(int time) {
        getContext().system().scheduler().scheduleOnce(
                Duration.create(time, TimeUnit.MILLISECONDS),
                getSelf(),
                new CoordinatorServerMessage.Timeout(), // the message to send
                getContext().system().dispatcher(), getSelf()
        );
    }

    abstract void fixDecision(Transaction transaction, CoordinatorServerMessage.Decision d);

    boolean hasDecided(Transaction transaction) {
        return transaction2decision.get(transaction) != null;
    } // has the node decided?

    // a simple logging function
    void print(String s) {
        System.out.format("%2d: %s\n", id, s);
    }

    @Override
    public Receive createReceive() {

        // Empty mapping: we'll define it in the inherited classes
        return receiveBuilder().build();
    }

    public Receive crashed() {
        return receiveBuilder()
                .match(CoordinatorServerMessage.Recovery.class, this::onRecovery)
                .match(Message.CheckerMsg.class, this::onCheckerMsg)
                .matchAny(msg -> {
                })
                .build();
    }

    private void onCheckerMsg(Message.CheckerMsg msg){
        this.checker = msg.checker;
    }

    public void onDecisionRequest(CoordinatorServerMessage.DecisionRequest msg) {  /* Decision Request */
        Transaction transaction = msg.transaction;
        if (hasDecided(transaction))
            getSender().tell(new CoordinatorServerMessage.DecisionResponse(transaction, transaction2decision.get(transaction)), getSelf());

        // just ignoring if we don't know the decision
    }
}
