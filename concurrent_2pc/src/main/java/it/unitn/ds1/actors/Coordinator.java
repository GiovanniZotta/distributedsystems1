package it.unitn.ds1.actors;

/*-- Coordinator -----------------------------------------------------------*/

import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.ds1.Main;
import it.unitn.ds1.transactions.CoordinatorTransaction;
import it.unitn.ds1.transactions.Transaction;
import it.unitn.ds1.messages.ClientCoordinatorMessage;
import it.unitn.ds1.messages.CoordinatorServerMessage;
import it.unitn.ds1.messages.Message;

import java.io.Serializable;
import java.util.*;

public class Coordinator extends Node {

    public enum CrashBefore2PC implements CrashPhase {BEFORE_TXN_ACCEPT_MSG, ON_CLIENT_MSG, ON_SERVER_MSG;
        public String toString() {
            return "CoordinatorCrashBefore2PC_" + name();
        }
    }
    public static class CrashDuring2PC {
        public enum CrashDuringVote {
            ZERO_MSG, RND_MSG, ALL_MSG;
            @Override
            public String toString() {
                return CrashDuring2PC.name() + "_CrashDuringVote_" + name();
            }
        }
        public enum CrashDuringDecision {
            ZERO_MSG, RND_MSG, ALL_MSG;
            @Override
            public String toString() {
                return CrashDuring2PC.name() + "_CrashDuringDecision_" + name();
            }
        }
        public static String name() {
            return "CoordinatorCrashDuring2PC";
        }
    }

    // here all the nodes that sent YES are collected
    private final Map<ActorRef, CoordinatorTransaction> client2transaction = new HashMap<>();
    private final Map<Transaction, ActorRef> transaction2client = new HashMap<>();
    private final Set<Transaction> pendingTransactions = new HashSet<>();

    boolean allVotedYes(CoordinatorTransaction transaction) { // returns true if all voted YES
        return transaction.getYesVoters().size() == transaction.getServers().size();
    }

    public Coordinator(int id, Set<CrashPhase> crashPhases) {
        super(id, crashPhases);
    }

    static public Props props(int id,  Set<CrashPhase> crashPhases) {
        return Props.create(Coordinator.class, () -> new Coordinator(id, crashPhases));
    }

    @Override
    public Receive createReceive() {

        return receiveBuilder()
                .match(CoordinatorServerMessage.RecoveryMsg.class, this::onRecoveryMsg)
                .match(Message.WelcomeMsg.class, this::onWelcomeMsg)
                .match(CoordinatorServerMessage.VoteResponse.class, this::onVoteResponse)
                .match(CoordinatorServerMessage.TimeoutMsg.class, this::onTimeoutMsg)
                .match(CoordinatorServerMessage.DecisionRequest.class, this::onDecisionRequest)
                .match(ClientCoordinatorMessage.TxnBeginMsg.class, this::onTxnBeginMsg)
                .match(ClientCoordinatorMessage.TxnEndMsg.class, this::onTxnEndMsg)
                .match(ClientCoordinatorMessage.ReadMsg.class, this::onReadMsg)
                .match(CoordinatorServerMessage.TxnReadResponseMsg.class, this::onTxnReadResponseMsg)
                .match(ClientCoordinatorMessage.WriteMsg.class, this::onWriteMsg)
                .match(Message.CheckCorrectness.class, this::onCheckCorrectness)
                .build();
    }

    void multicastAndCrash(Serializable m, int recoverIn){
//        Integer crashAfter = 0;
//        switch(phase){
//            case ZERO_MESSAGES:
//                if(Main.DEBUG_MULT_CRASH_ZERO){
//                    print("CRASH AFTER ZERO MESSAGES");
//                }
//                crash(recoverIn);
//                return;
//            case RANDOM_MESSAGES:
//                crashAfter = r.nextInt(Main.N_SERVER - 1);
//                if(Main.DEBUG_MULT_CRASH_RANDOM)
//                    print("CRASH AFTER " + crashAfter + " MESSAGES");
//                break;
//            case ALL_MESSAGES:
//                crashAfter = Main.N_SERVER - 1;
//                if(Main.DEBUG_MULT_CRASH_ALL)
//                    print("CRASH AFTER ALL MESSAGES");
//                break;
//        }
//        Integer i = 0;
//        for (ActorRef p : servers) {
//            p.tell(m, getSelf());
//            if(i == crashAfter){
//                crash(recoverIn);
//                return;
//            }
//            i++;
//        }
    }

    public void onWelcomeMsg(Message.WelcomeMsg msg) {                   /* Start */
        setGroup(msg);
    }

    boolean hasDecided(Transaction transaction) {
        return transaction2decision.get(transaction) != null;
    }

    // fix the final decision of the current node
    void fixDecision(CoordinatorTransaction transaction, CoordinatorServerMessage.Decision d) {
        if (!hasDecided(transaction)) {
            transaction2decision.put(transaction, d);
            transaction.setState(Transaction.State.DECIDED);
            ActorRef client = transaction2client.get(transaction);
            assert(client != null);
            sendMessage(client,
                    new ClientCoordinatorMessage.TxnResultMsg(
                            d == CoordinatorServerMessage.Decision.COMMIT,
                            transaction.getNumAttemptedTxn()));
            pendingTransactions.remove(transaction);
            client2transaction.remove(client);
            transaction2client.remove(transaction);
            // TODO: remove client from map
            if(Main.COORD_DEBUG_DECISION)
                print("Coordinator " + this.id + " decided " + d
                + " on transaction " + transaction.getTxnId());
        }
    }

    private CoordinatorTransaction getCTfromTransaction(Transaction transaction){
        ActorRef c = transaction2client.get(transaction);
        return client2transaction.get(c);
    }

    public void onVoteResponse(CoordinatorServerMessage.VoteResponse msg) {                    /* Vote */
        if (hasDecided(msg.transaction)) {
            // we have already decided and sent the decision to the group,
            // so do not care about other votes
            return;
        }
        CoordinatorTransaction transaction = getCTfromTransaction(msg.transaction);
        CoordinatorServerMessage.Vote v = (msg).vote;
        if (v == CoordinatorServerMessage.Vote.YES) {
            transaction.getYesVoters().add(getSender());
            if (allVotedYes(transaction)) {
                takeDecision(transaction, CoordinatorServerMessage.Decision.COMMIT);
                //if (id==-1) {crash(3000); return;}
//                multicastAndCrash(new CoordinatorServerMessages.DecisionResponse(transaction2decision.get(transaction)), 3000);
            }
        } else { // a NO vote
            // on a single NO we decide ABORT
            takeDecision(transaction, CoordinatorServerMessage.Decision.ABORT);
        }
    }

    public void onTimeoutMsg(CoordinatorServerMessage.TimeoutMsg msg) {
        unsetTimeout(msg.transaction);
//        if (!hasDecided(tra)) {
//            print("Timeout");
//
//            // TODO 1: coordinator timeout action
//            // abort to avoid blocking
//        }
    }


    @Override
    public void onRecoveryMsg(CoordinatorServerMessage.RecoveryMsg msg) {
        getContext().become(createReceive());

        for (Transaction t : new HashSet<>(pendingTransactions)) {
            takeDecision(t, CoordinatorServerMessage.Decision.ABORT);
        }


    }

    public void onTxnBeginMsg(ClientCoordinatorMessage.TxnBeginMsg msg) {
        // initialize transaction
        // TODO: check if the client is already in the map
        CoordinatorTransaction t = new CoordinatorTransaction(msg.clientId, msg.numAttemptedTxn, getSender());
        client2transaction.put(getSender(), t);
        transaction2client.put(t, getSender());
        pendingTransactions.add(t);
        // send accept
        if (!maybeCrash(CrashBefore2PC.BEFORE_TXN_ACCEPT_MSG))
            getSender().tell(new ClientCoordinatorMessage.TxnAcceptMsg(), getSelf());
    }

    private void takeDecision(Transaction transaction, CoordinatorServerMessage.Decision decision){
        unsetTimeout(transaction);
        CoordinatorTransaction transaction1 = getCTfromTransaction(transaction);
        fixDecision(transaction1, decision);
        multicast(new CoordinatorServerMessage.DecisionResponse(transaction1, transaction2decision.get(transaction1)), transaction1.getServers());
    }

    public void onTxnEndMsg(ClientCoordinatorMessage.TxnEndMsg msg) {
        CoordinatorTransaction transaction = client2transaction.get(getSender());
        if(msg.commit){
            multicast(new CoordinatorServerMessage.VoteRequest(transaction), transaction.getServers());
            transaction.setState(Transaction.State.READY);
        } else {
            takeDecision(transaction, CoordinatorServerMessage.Decision.ABORT);
        }
//        print("Sending vote requests");
//        multicast(new CoordinatorServerMessages.VoteRequest());
//      multicastAndCrash(new VoteRequest(), 3000);
//        setTimeout(Main.VOTE_TIMEOUT);
//      crash(5000);
    }

    private Boolean trackServerForTxn(CoordinatorTransaction transaction, Integer serverId) {
        transaction.getServers().add(servers.get(serverId));
        return maybeCrash(CrashBefore2PC.ON_CLIENT_MSG);
    }

    public void onReadMsg (ClientCoordinatorMessage.ReadMsg msg) {
        int key = msg.key;
        int serverId = key / Server.DB_SIZE;
        CoordinatorTransaction transaction = client2transaction.get(getSender());
        if (!trackServerForTxn(transaction, serverId))
            sendMessage(servers.get(serverId), new CoordinatorServerMessage.TransactionRead(transaction, key), true);
    }

    public void onTxnReadResponseMsg(CoordinatorServerMessage.TxnReadResponseMsg msg) {
        unsetTimeout(msg.transaction);
        ActorRef c = transaction2client.get(msg.transaction);
        if (!maybeCrash(CrashBefore2PC.ON_SERVER_MSG))
            c.tell(new ClientCoordinatorMessage.ReadResultMsg(msg.key, msg.valueRead), getSelf());
    }

    public void onWriteMsg (ClientCoordinatorMessage.WriteMsg msg) {
        int key = msg.key;
        int value = msg.value;
        int serverId = key / Server.DB_SIZE;
        CoordinatorTransaction transaction = client2transaction.get(getSender());
        if (!trackServerForTxn(transaction, serverId))
            sendMessage(servers.get(serverId), new CoordinatorServerMessage.TransactionWrite(transaction, key, value), true);
    }

    @Override
    public void onCheckCorrectness(Message.CheckCorrectness msg){
        getSender().tell(new Message.CheckCorrectnessResponse(id, null, numCrashes), getSelf());
    }

}
