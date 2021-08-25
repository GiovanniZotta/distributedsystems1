package it.unitn.ds1.actors;

/*-- Common functionality for both Coordinator and Participants ------------*/

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import it.unitn.ds1.Main;
import it.unitn.ds1.messages.CoordinatorServerMessage;
import it.unitn.ds1.messages.Message;
import it.unitn.ds1.transactions.Transaction;
import scala.concurrent.duration.Duration;

import java.util.*;
import java.util.concurrent.TimeUnit;

public abstract class Node extends AbstractActor {
    public interface CrashPhase {
    }

    public static class CrashPhaseMap extends HashMap<CrashPhase, Integer> {

        public CrashPhaseMap() {
            super();
        }

        public CrashPhaseMap(CrashPhaseMap numCrashes) {
            super(numCrashes);
        }

        @Override
        public String toString() {
            StringBuilder res = new StringBuilder();
            for (Entry<CrashPhase, Integer> entry : this.entrySet()) {
                res.append(entry.getKey().toString()).append(": ").append(entry.getValue()).append("\n");
            }
            return res.toString();
        }

        public static CrashPhaseMap sumMaps(Collection<CrashPhaseMap> maps) {
            CrashPhaseMap res = new CrashPhaseMap();
            for (CrashPhaseMap map : maps)
                for (Entry<CrashPhase, Integer> entry : map.entrySet())
                    res.put(entry.getKey(), res.getOrDefault(entry.getKey(), 0) + entry.getValue());
            return res;
        }
    }

    public class CrashException extends Exception {
    }

    protected int id;                       // node ID

    // remember the decision taken for each transaction
    protected final Map<Transaction, CoordinatorServerMessage.Decision> transaction2decision;
    // phases where we may crash
    protected final Set<CrashPhase> crashPhases;
    // count the number of crashes in each phase
    protected final CrashPhaseMap numCrashes;
    // transactions that have not been decided yet
    protected final Set<Transaction> pendingTransactions = new HashSet<>();
    // RNG
    protected final Random r;


    public Node(int id, Set<CrashPhase> crashPhases) {
        super();
        this.id = id;
        this.crashPhases = crashPhases;
        transaction2decision = new HashMap<>();
        numCrashes = new CrashPhaseMap();
        r = new Random();
    }

    @Override
    public Receive createReceive() {
        // Empty mapping: we'll define it in the inherited classes
        return receiveBuilder().build();
    }

    public Receive crashed() {
        return receiveBuilder()
                .match(CoordinatorServerMessage.RecoveryMsg.class, this::onRecoveryMsg)
                .match(Message.CheckCorrectness.class, this::onCheckCorrectness)
                .matchAny(msg -> {
                })
                .build();
    }

    // emulate a crash and a recovery in a given time
    protected void crash(int recoverIn, CrashPhase crashPhase) throws CrashException {
        getContext().become(crashed());
        numCrashes.put(crashPhase, numCrashes.getOrDefault(crashPhase, 0) + 1);
        if (Main.NODE_DEBUG_CRASH)
            print("CRASH IN PHASE " + crashPhase);

        // setting a timer to "recover"
        getContext().system().scheduler().scheduleOnce(
                Duration.create(recoverIn, TimeUnit.MILLISECONDS),
                getSelf(),
                new CoordinatorServerMessage.RecoveryMsg(), // message sent to myself
                getContext().system().dispatcher(), getSelf()
        );
        throw new CrashException();
    }

    // crash with a certain probability if the node was told to crash in this crashPhase
    protected void maybeCrash(CrashPhase crashPhase) throws CrashException {
        double crash_prob = getClass().equals(Coordinator.class) ? Main.COORD_CRASH_PROBABILITY : Main.SERVER_CRASH_PROBABILITY;
        if (crashPhases.contains(crashPhase) && r.nextDouble() < crash_prob) {
            crash(Main.MIN_RECOVERY_TIME + r.nextInt(Main.MAX_RECOVERY_TIME - Main.MIN_RECOVERY_TIME), crashPhase);
        }
    }

    // emulate a delay of d milliseconds
    void delay(int d) {
        try {
            Thread.sleep(d);
        } catch (Exception ignored) {
        }
    }

    protected abstract boolean hasDecided(Transaction transaction);

    // a simple logging function
    void print(String s) {
        if (this.getClass().equals(Server.class))
            System.out.format("Server      %2d: %s\n", id, s);
        else
            System.out.format("Coordinator %2d: %s\n", id, s);
    }

    // send a message to 'to'
    protected void sendMessage(ActorRef to, Message msg) {
        // simulate network delay
        Integer delay = r.nextInt(Main.MAX_NODE_DELAY);
        try {
            Thread.sleep(delay);
            to.tell(msg, getSelf());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    protected void reply(Message msg) {
        sendMessage(getSender(), msg);
    }

    // schedule a timeout for the transaction and return it
    protected Cancellable newTimeout(int time, Transaction transaction) {
        return getContext().system().scheduler().scheduleOnce(
                Duration.create(time, TimeUnit.MILLISECONDS),
                getSelf(),
                new CoordinatorServerMessage.TimeoutMsg(transaction), // the message to send
                getContext().system().dispatcher(), getSelf()
        );
    }

    // return, if exists in the given class, a crashPhase before sending any message
    CrashPhase getZeroMsgCrashPhase(Class crashPhaseClass) {
        if (Coordinator.CrashDuring2PC.CrashDuringVote.class.equals(crashPhaseClass)) {
            return Coordinator.CrashDuring2PC.CrashDuringVote.ZERO_MSG;
        } else if (Coordinator.CrashDuring2PC.CrashDuringDecision.class.equals(crashPhaseClass)) {
            return Coordinator.CrashDuring2PC.CrashDuringDecision.ZERO_MSG;
        } else if (Server.CrashDuring2PC.CrashDuringVote.class.equals(crashPhaseClass)) {
            return Server.CrashDuring2PC.CrashDuringVote.NO_VOTE;
        } else if (Server.CrashDuring2PC.CrashDuringTermination.class.equals(crashPhaseClass)) {
            return Server.CrashDuring2PC.CrashDuringTermination.NO_REPLY;
        } else {
            return null;
        }
    }

    // return, if exists in the given class, a crashPhase before sending all the messages
    CrashPhase getAllMsgCrashPhase(Class crashPhaseClass) {
        if (Coordinator.CrashDuring2PC.CrashDuringVote.class.equals(crashPhaseClass)) {
            return Coordinator.CrashDuring2PC.CrashDuringVote.ALL_MSG;
        } else if (Coordinator.CrashDuring2PC.CrashDuringDecision.class.equals(crashPhaseClass)) {
            return Coordinator.CrashDuring2PC.CrashDuringDecision.ALL_MSG;
        } else if (Server.CrashDuring2PC.CrashDuringVote.class.equals(crashPhaseClass)) {
            return Server.CrashDuring2PC.CrashDuringVote.AFTER_VOTE;
        } else if (Server.CrashDuring2PC.CrashDuringTermination.class.equals(crashPhaseClass)) {
            return Server.CrashDuring2PC.CrashDuringTermination.ALL_REPLY;
        } else {
            return null;
        }
    }

    // return, if exists in the given class, a crashPhase before sending a random number of messages
    CrashPhase getRndMsgCrashPhase(Class crashPhaseClass) {
        if (Coordinator.CrashDuring2PC.CrashDuringVote.class.equals(crashPhaseClass)) {
            return Coordinator.CrashDuring2PC.CrashDuringVote.RND_MSG;
        } else if (Coordinator.CrashDuring2PC.CrashDuringDecision.class.equals(crashPhaseClass)) {
            return Coordinator.CrashDuring2PC.CrashDuringDecision.RND_MSG;
        } else if (Server.CrashDuring2PC.CrashDuringTermination.class.equals(crashPhaseClass)) {
            return Server.CrashDuring2PC.CrashDuringTermination.RND_REPLY;
        } else {
            return null;
        }
    }

    // abstract method to be implemented in extending classes
    protected abstract void onRecoveryMsg(CoordinatorServerMessage.RecoveryMsg msg);


    // on termination protocol answer if the decision is known
    public void onDecisionRequest(CoordinatorServerMessage.DecisionRequest msg) {  /* Decision Request */
        Transaction transaction = msg.transaction;
        if (hasDecided(transaction))
            reply(new CoordinatorServerMessage.DecisionResponse(transaction, transaction2decision.get(transaction)));
        // just ignore if we don't know the decision
    }

    public abstract void onCheckCorrectness(Message.CheckCorrectness msg);

}
