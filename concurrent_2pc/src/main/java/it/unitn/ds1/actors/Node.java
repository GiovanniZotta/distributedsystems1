package it.unitn.ds1.actors;

/*-- Common functionality for both Coordinator and Participants ------------*/

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import it.unitn.ds1.Main;
import it.unitn.ds1.transactions.Transaction;
import it.unitn.ds1.messages.CoordinatorServerMessages;
import it.unitn.ds1.messages.Message;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;

public abstract class Node extends AbstractActor {

    public enum CrashPhase {ZERO_MESSAGES, RANDOM_MESSAGES, ALL_MESSAGES}
    protected int id;                           // node ID
    protected List<ActorRef> servers;      // list of participant nodes
    protected ActorRef checker;
    protected final Map<Transaction, CoordinatorServerMessages.Decision> transaction2decision;
    protected final Random r;

    public Node(int id) {
        super();
        this.id = id;
        transaction2decision = new HashMap<>();
        r = new Random();
    }

    // abstract method to be implemented in extending classes
    protected abstract void onRecovery(CoordinatorServerMessages.Recovery msg);

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
    void crash(int recoverIn) {
        getContext().become(crashed());
        if(Main.NODE_DEBUG_CRASH)
            print("CRASH!!!");

        // setting a timer to "recover"
        getContext().system().scheduler().scheduleOnce(
                Duration.create(recoverIn, TimeUnit.MILLISECONDS),
                getSelf(),
                new CoordinatorServerMessages.Recovery(), // message sent to myself
                getContext().system().dispatcher(), getSelf()
        );
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
    void abstract multicastAndCrash(Serializable m, int recoverIn, CrashPhase phase);

    // schedule a Timeout message in specified time
    void setTimeout(int time) {
        getContext().system().scheduler().scheduleOnce(
                Duration.create(time, TimeUnit.MILLISECONDS),
                getSelf(),
                new CoordinatorServerMessages.Timeout(), // the message to send
                getContext().system().dispatcher(), getSelf()
        );
    }

    abstract void fixDecision(Transaction transaction, CoordinatorServerMessages.Decision d);

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
                .match(CoordinatorServerMessages.Recovery.class, this::onRecovery)
                .match(Message.CheckerMsg.class, this::onCheckerMsg)
                .matchAny(msg -> {
                })
                .build();
    }

    private void onCheckerMsg(Message.CheckerMsg msg){
        this.checker = msg.checker;
    }

    public void onDecisionRequest(CoordinatorServerMessages.DecisionRequest msg) {  /* Decision Request */
        Transaction transaction = msg.transaction;
        if (hasDecided(transaction))
            getSender().tell(new CoordinatorServerMessages.DecisionResponse(transaction, transaction2decision.get(transaction)), getSelf());

        // just ignoring if we don't know the decision
    }
}
