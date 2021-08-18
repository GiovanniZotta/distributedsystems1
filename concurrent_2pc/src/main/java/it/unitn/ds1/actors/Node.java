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

        public CrashPhaseMap() {
            super();
        }

        public CrashPhaseMap(CrashPhaseMap numCrashes) {
            super(numCrashes);
        }

        @Override
        public String toString() {
            String res = "";
            for (Entry<CrashPhase, Integer> entry : this.entrySet()){
                res += entry.getKey().toString() + ": " + entry.getValue() + "\n";
            }
            return res;
        }

        public static CrashPhaseMap sumMaps(Collection<CrashPhaseMap> maps) {
            CrashPhaseMap res = new CrashPhaseMap();
            for (CrashPhaseMap map : maps)
                for (Entry<CrashPhase, Integer> entry : map.entrySet())
                    res.put(entry.getKey(), res.getOrDefault(entry.getKey(), 0) + entry.getValue());
            return res;
        }
    }

    protected static final double CRASH_PROBABILITY = 0.001;
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
    protected abstract void onRecoveryMsg(CoordinatorServerMessage.RecoveryMsg msg);

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
                new CoordinatorServerMessage.RecoveryMsg(), // message sent to myself
                getContext().system().dispatcher(), getSelf()
        );
    }

    Boolean maybeCrash(CrashPhase crashPhase) {
        if (crashPhases.contains(crashPhase) && r.nextDouble() < CRASH_PROBABILITY) {
            crash(600 + r.nextInt(Main.MAX_RECOVERY_TIME), crashPhase);
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

    void multicast(CoordinatorServerMessage m, Collection<ActorRef> group) {
        multicast(m, group, false);
    }
    void multicast(CoordinatorServerMessage m, Collection<ActorRef> group, Boolean setTimeout) {
        for (ActorRef p : group)
            sendMessage(p, m, setTimeout);
    }

    // a multicast implementation that crashes after sending the first message
    abstract void multicastAndCrash(Serializable m, int recoverIn);

    // schedule a Timeout message in specified time
    void setTimeout(int time, Transaction transaction) {
        transaction.setTimeout(getContext().system().scheduler().scheduleOnce(
                Duration.create(time, TimeUnit.MILLISECONDS),
                getSelf(),
                new CoordinatorServerMessage.TimeoutMsg(transaction), // the message to send
                getContext().system().dispatcher(), getSelf()
        ));
    }

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
                .match(CoordinatorServerMessage.RecoveryMsg.class, this::onRecoveryMsg)
                .match(Message.CheckerMsg.class, this::onCheckerMsg)
                .match(Message.CheckCorrectness.class, this::onCheckCorrectness)
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
            reply(new CoordinatorServerMessage.DecisionResponse(transaction, transaction2decision.get(transaction)));

        // just ignoring if we don't know the decision
    }

    public abstract void onCheckCorrectness(Message.CheckCorrectness msg);

    protected void sendMessage(ActorRef to, CoordinatorServerMessage msg, Boolean setTimeout) {
        to.tell(msg, getSelf());
        if (setTimeout)
            setTimeout(Main.TIMEOUT, msg.transaction);
    }

    protected void sendMessage(ActorRef to, Message msg) {
        to.tell(msg, getSelf());
    }

    protected void reply(CoordinatorServerMessage msg, Boolean setTimeout) {
        sendMessage(getSender(), msg, setTimeout);
    }

    protected void reply(Message msg) {
        sendMessage(getSender(), msg);
    }

    protected void unsetTimeout(Transaction transaction) {
        if (transaction.getTimeout() != null)
            transaction.getTimeout().cancel();
        transaction.setTimeout(null);
    }
}
