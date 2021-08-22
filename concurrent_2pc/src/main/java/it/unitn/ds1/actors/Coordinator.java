package it.unitn.ds1.actors;

/*-- Coordinator -----------------------------------------------------------*/

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import it.unitn.ds1.Main;
import it.unitn.ds1.transactions.CoordinatorTransaction;
import it.unitn.ds1.transactions.Transaction;
import it.unitn.ds1.messages.ClientCoordinatorMessage;
import it.unitn.ds1.messages.CoordinatorServerMessage;
import it.unitn.ds1.messages.Message;

import java.util.*;

public class Coordinator extends Node {

    public enum CrashBefore2PC implements CrashPhase {
        BEFORE_TXN_ACCEPT_MSG, ON_CLIENT_MSG, ON_SERVER_MSG;

        public String toString() {
            return "CoordinatorCrashBefore2PC_" + name();
        }
    }

    public static class CrashDuring2PC {
        public enum CrashDuringVote implements CrashPhase {
            ZERO_MSG, RND_MSG, ALL_MSG;

            @Override
            public String toString() {
                return CrashDuring2PC.name() + "_CrashDuringVote_" + name();
            }
        }

        public enum CrashDuringDecision implements CrashPhase {
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


    boolean allVotedYes(CoordinatorTransaction transaction) { // returns true if all voted YES
        return transaction.getYesVoters().size() == transaction.getServers().size();
    }

    public Coordinator(int id, Set<CrashPhase> crashPhases) {
        super(id, crashPhases);
    }

    static public Props props(int id, Set<CrashPhase> crashPhases) {
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
            sendMessage(client,
                    new ClientCoordinatorMessage.TxnResultMsg(
                            transaction.getClientId(), transaction.getNumAttemptedTxn(),
                            d == CoordinatorServerMessage.Decision.COMMIT));
            pendingTransactions.remove(transaction);
            client2transaction.remove(client);
            transaction2client.remove(transaction);
            if (Main.COORD_DEBUG_DECISION)
                print("DECIDED " + d
                        + " ON TXN " + transaction.getTxnId());
        }
    }

    private CoordinatorTransaction getCTfromTransaction(Transaction transaction) {
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
        if(Main.COORD_DEBUG_RECEIVED_VOTE)
            print("RECEIVED VOTE " + v.toString() + " FROM SERVER " + servers.indexOf(getSender()));
        if (v == CoordinatorServerMessage.Vote.YES) {
            transaction.getYesVoters().add(getSender());
            if (allVotedYes(transaction)) {
                if(Main.COORD_DEBUG_ALL_VOTED_YES)
                    print("ALL VOTED YES");
                try {
                    takeDecision(transaction, CoordinatorServerMessage.Decision.COMMIT);
                } catch (CrashException ignored) {}
                //if (id==-1) {crash(3000); return;}
//                multicastAndCrash(new CoordinatorServerMessages.DecisionResponse(transaction2decision.get(transaction)), 3000);
            }
        } else { // a NO vote
            // on a single NO we decide ABORT
            try {
                takeDecision(transaction, CoordinatorServerMessage.Decision.ABORT);
            } catch (CrashException e) {}
        }
    }

    public void onTimeoutMsg(CoordinatorServerMessage.TimeoutMsg msg) {
        if(Main.COORD_DEBUG_TIMEOUT)
            print("TIMEOUT FOR TRANSACTION " + msg.transaction.getTxnId());
        CoordinatorTransaction t = getCTfromTransaction(msg.transaction);
        if (t == null)
            return;
        unsetTimeout(t);

        // if in INIT -> server crashed before 2PC
        // if in READY -> at least one server did not respond to the vote request (and nobody voted abort)
        assert t.getState() != Transaction.State.DECIDED;
        try {
            takeDecision(t, CoordinatorServerMessage.Decision.ABORT);
        } catch (CrashException e) {}
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

        if(Main.COORD_DEBUG_RECOVERY)
            print("RECOVERED");

        try {
            for (Transaction t : new HashSet<>(pendingTransactions)) {
                takeDecision(t, CoordinatorServerMessage.Decision.ABORT);
            }
        } catch (CrashException e){}
    }

    public void onTxnBeginMsg(ClientCoordinatorMessage.TxnBeginMsg msg) {
        // initialize transaction
        CoordinatorTransaction t = client2transaction.get(getSender());

        try {
            if (t != null)
                takeDecision(t, CoordinatorServerMessage.Decision.ABORT);

            t = new CoordinatorTransaction(msg.clientId, msg.numAttemptedTxn, getSender());
            client2transaction.put(getSender(), t);
            transaction2client.put(t, getSender());
            pendingTransactions.add(t);
            if(Main.COORD_DEBUG_BEGIN_TXN)
                print("GOT TXN BEGIN FROM " + msg.clientId + ", NEW_TXN: " + msg.numAttemptedTxn + ", OLD_TXN: " + (t != null ? t.getTxnId() : "null"));
            // send accept

            maybeCrash(CrashBefore2PC.BEFORE_TXN_ACCEPT_MSG);
            reply(new ClientCoordinatorMessage.TxnAcceptMsg(msg.clientId, msg.numAttemptedTxn));
        } catch (CrashException e) {}
    }

    private void takeDecision(Transaction transaction, CoordinatorServerMessage.Decision decision) throws CrashException {
        if(Main.COORD_DEBUG_DECISION)
            print("TAKING DECISION FOR TXN " + transaction.getTxnId() + ", OLD DECISION: " + transaction2decision.get(transaction));
        CoordinatorTransaction transaction1 = getCTfromTransaction(transaction);
        unsetTimeout(transaction1);
        fixDecision(transaction1, decision);
        multicast(new CoordinatorServerMessage.DecisionResponse(transaction1, transaction2decision.get(transaction1)), transaction1.getServers(), false, CrashDuring2PC.CrashDuringDecision.class);
    }

    public void onTxnEndMsg(ClientCoordinatorMessage.TxnEndMsg msg) {
        CoordinatorTransaction transaction = client2transaction.get(getSender());
        if (isCurrentTransaction(transaction, msg)) {
            try{
                if (msg.commit) {
                    if(Main.COORD_DEBUG_BEGIN_VOTE)
                        print("SENDING VOTE REQUEST");

                    multicast(new CoordinatorServerMessage.VoteRequest(transaction, transaction.getServers()), transaction.getServers(),
                            true, CrashDuring2PC.CrashDuringVote.class);
                    transaction.setState(Transaction.State.READY);
                } else {
                    takeDecision(transaction, CoordinatorServerMessage.Decision.ABORT);
                }
            } catch (CrashException e) {}
        }
    }

    private void trackServerForTxn(CoordinatorTransaction transaction, Integer serverId) throws CrashException {
        transaction.getServers().add(servers.get(serverId));
        maybeCrash(CrashBefore2PC.ON_CLIENT_MSG);
    }

    public void onReadMsg(ClientCoordinatorMessage.ReadMsg msg) {
        if(Main.COORD_DEBUG_READ){
            print("READING KEY " + msg.key);
        }
        CoordinatorTransaction transaction = client2transaction.get(getSender());
        if (isCurrentTransaction(transaction, msg)) {
            int key = msg.key;
            int serverId = key / Server.DB_SIZE;
            try {
                trackServerForTxn(transaction, serverId);
                sendMessage(servers.get(serverId), new CoordinatorServerMessage.TransactionRead(transaction, key), true);
            } catch (CrashException e){
            }
        }
    }

    public void onTxnReadResponseMsg(CoordinatorServerMessage.TxnReadResponseMsg msg) {
        unsetTimeout(msg.transaction, getSender());
        ActorRef c = transaction2client.get(msg.transaction);
        try{
            maybeCrash(CrashBefore2PC.ON_SERVER_MSG);
            sendMessage(c, new ClientCoordinatorMessage.ReadResultMsg(
                    msg.transaction.getClientId(),
                    msg.transaction.getNumAttemptedTxn(),
                    msg.key,
                    msg.valueRead));
            if(Main.COORD_DEBUG_READ_RESPONSE)
                print("REPLYING WITH VALUE " + msg.valueRead + " FOR KEY " + msg.key);
        } catch (CrashException e) {}

    }

    public void onWriteMsg(ClientCoordinatorMessage.WriteMsg msg) {
        CoordinatorTransaction transaction = client2transaction.get(getSender());
        if (isCurrentTransaction(transaction, msg)) {
            int key = msg.key;
            int value = msg.value;
            int serverId = key / Server.DB_SIZE;
            try {
                trackServerForTxn(transaction, serverId);
                sendMessage(servers.get(serverId), new CoordinatorServerMessage.TransactionWrite(transaction, key, value));
            } catch (CrashException e) {}
        }
    }

    private Boolean isCurrentTransaction(CoordinatorTransaction transaction, ClientCoordinatorMessage msg) {
        return transaction != null && transaction.getNumAttemptedTxn().equals(msg.numAttemptedTxn);
    }

    @Override
    public void onCheckCorrectness(Message.CheckCorrectness msg) {
        reply(new Message.CheckCorrectnessResponse(id, null, numCrashes));
        getContext().stop(getSelf());
    }

    void setTimeout(int time, Transaction transaction, ActorRef server) {
        if(Main.COORD_DEBUG_SET_TIMEOUT)
            print("SET TIMEOUT FOR TRANSACTION " + transaction.getTxnId() + " FOR SERVER " + servers.indexOf(server));
        CoordinatorTransaction t = getCTfromTransaction(transaction);
        t.pushServerTimeout(server, newTimeout(time, t));
    }

    protected void unsetTimeout(Transaction transaction, ActorRef server) {
        CoordinatorTransaction t = getCTfromTransaction(transaction);
        if (t != null && t.hasTimeout(server)) {
            Cancellable to = t.popOldestServerTimeout(server);
            if (!to.isCancelled()) {
                if (!to.cancel()) {
//                    print("ERRORE FORTISSIMO!!!!");
//                    throw new NullPointerException();
//                    to.
                } else {
                    if(Main.COORD_DEBUG_UNSET_TIMEOUT)
                        print("UNSET TIMEOUT FOR TRANSACTION " + transaction.getTxnId() + " FOR SERVER " + servers.indexOf(server));
                }
            }
        }
    }

    private void unsetTimeout(Transaction transaction) {

        CoordinatorTransaction t = getCTfromTransaction(transaction);
        if (t == null) return;
        for (ActorRef s : t.getServers()) {
            while (t.hasTimeout(s))
                unsetTimeout(t, s);
        }
    }

    protected void sendMessage(ActorRef to, CoordinatorServerMessage msg, Boolean setTimeout) {
        super.sendMessage(to, msg);
        if (setTimeout)
            setTimeout(Main.COORD_TIMEOUT, msg.transaction, to);
    }

    void multicast(CoordinatorServerMessage m, Collection<ActorRef> group, Boolean setTimeout, Class phase) throws CrashException {
        CrashPhase zeroMsg = getZeroMsgCrashPhase(phase);
        CrashPhase rndMsg = getRndMsgCrashPhase(phase);
        CrashPhase allMsg = getAllMsgCrashPhase(phase);

        if (zeroMsg != null)
            maybeCrash(zeroMsg);
        for (ActorRef p : group) {
            if (rndMsg != null)
                maybeCrash(rndMsg);
            sendMessage(p, m, setTimeout);
        }
        if (allMsg != null)
            maybeCrash(allMsg);
    }
}
