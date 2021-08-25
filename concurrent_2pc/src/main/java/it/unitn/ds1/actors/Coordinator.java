package it.unitn.ds1.actors;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import it.unitn.ds1.Main;
import it.unitn.ds1.messages.ClientCoordinatorMessage;
import it.unitn.ds1.messages.CoordinatorServerMessage;
import it.unitn.ds1.messages.Message;
import it.unitn.ds1.transactions.CoordinatorTransaction;
import it.unitn.ds1.transactions.Transaction;

import java.util.*;

public class Coordinator extends Node {

    //------------------ Definition of coordinator crash phases ------------------
    public enum CrashBefore2PC implements CrashPhase {
        BEFORE_TXN_ACCEPT_MSG, ON_CLIENT_MSG, ON_SERVER_MSG;

        public String toString() {
            return "CoordinatorCrashBefore2PC_" + name();
        }
    }

    public static class CrashDuring2PC {
        public static String name() {
            return "CoordinatorCrashDuring2PC";
        }

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
    }

    // associate client to its current transaction and vice-versa
    private final Map<ActorRef, CoordinatorTransaction> client2transaction = new HashMap<>();
    private final Map<Transaction, ActorRef> transaction2client = new HashMap<>();
    // servers in the system
    protected List<ActorRef> servers;


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

    // remember the servers
    void setGroup(Message.WelcomeMsg sm) {
        servers = new ArrayList<>(sm.group);
        if (Main.NODE_DEBUG_STARTING_SIZE)
            print("STARTING WITH " + sm.group.size() + " PEER(S)");
    }

    private boolean allVotedYes(CoordinatorTransaction transaction) { // returns true if all voted YES
        return transaction.getYesVoters().size() == transaction.getServers().size();
    }

    @Override
    protected boolean hasDecided(Transaction transaction) {
        return transaction == null || transaction2decision.get(transaction) != null;
    }

    private Boolean isCurrentTransaction(CoordinatorTransaction transaction, ClientCoordinatorMessage msg) {
        return transaction != null && transaction.getNumAttemptedTxn().equals(msg.numAttemptedTxn);
    }

    // get the information relative to the given transaction
    private CoordinatorTransaction getCTfromTransaction(Transaction transaction) {
        ActorRef c = transaction2client.get(transaction);
        return client2transaction.get(c);
    }

    // set timeout for a server to answer a request from a given transaction
    private void setTimeout(int time, Transaction transaction, ActorRef server) {
        if (Main.COORD_DEBUG_SET_TIMEOUT)
            print("SET TIMEOUT FOR TRANSACTION " + transaction.getTxnId() + " FOR SERVER " + servers.indexOf(server));
        CoordinatorTransaction t = getCTfromTransaction(transaction);
        t.pushServerTimeout(server, newTimeout(time, t));
    }

    // unset the 'oldest' timeout we set for the server for this transaction
    private void unsetTimeout(Transaction transaction, ActorRef server) {
        CoordinatorTransaction t = getCTfromTransaction(transaction);
        if (t != null && t.hasTimeout(server)) {
            Cancellable to = t.popOldestServerTimeout(server);
            to.cancel();
            if (Main.COORD_DEBUG_UNSET_TIMEOUT)
                print("UNSET TIMEOUT FOR TRANSACTION " + transaction.getTxnId() + " FOR SERVER " + servers.indexOf(server));
        }
    }

    // unset all the timeouts we set for this transaction
    private void unsetTimeout(Transaction transaction) {
        CoordinatorTransaction t = getCTfromTransaction(transaction);
        if (t == null) return;
        for (ActorRef s : t.getServers()) {
            while (t.hasTimeout(s))
                unsetTimeout(t, s);
        }
    }

    // send a message to 'to' and optionally set a timeout
    protected void sendMessage(ActorRef to, CoordinatorServerMessage msg, Boolean setTimeout) {
        super.sendMessage(to, msg);
        if (setTimeout)
            setTimeout(Main.COORD_TIMEOUT, msg.transaction, to);
    }

    // send a message to every actor in the group and optionally set a timeout. We may crash during the sending
    // according to the crashPhases told to the coordinator
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

    // fix the final decision of the current node
    void fixDecision(CoordinatorTransaction transaction, CoordinatorServerMessage.Decision d) {
        if (!hasDecided(transaction)) {
            transaction2decision.put(transaction, d);
            transaction.setState(Transaction.State.DECIDED);
            ActorRef client = transaction2client.get(transaction);
            // tell the client the decision
            sendMessage(client,
                    new ClientCoordinatorMessage.TxnResultMsg(
                            transaction.getClientId(), transaction.getNumAttemptedTxn(),
                            d == CoordinatorServerMessage.Decision.COMMIT));
            // remove from the current transactions
            pendingTransactions.remove(transaction);
            client2transaction.remove(client);
            transaction2client.remove(transaction);
            if (Main.COORD_DEBUG_DECISION)
                print("DECIDED " + d
                        + " ON TXN " + transaction.getTxnId());
        }
    }

    private void takeDecision(Transaction transaction, CoordinatorServerMessage.Decision decision) throws CrashException {
        if (Main.COORD_DEBUG_DECISION)
            print("TAKING DECISION FOR TXN " + transaction.getTxnId() + ", OLD DECISION: " + transaction2decision.get(transaction));
        CoordinatorTransaction transaction1 = getCTfromTransaction(transaction);
        unsetTimeout(transaction1);
        fixDecision(transaction1, decision);
        multicast(new CoordinatorServerMessage.DecisionResponse(
                        transaction1,
                        transaction2decision.get(transaction1)),
                transaction1.getServers(),
                false,
                CrashDuring2PC.CrashDuringDecision.class);
    }

    // called when we send a request to a server, add the server to the list of servers handling the given transaction
    private void trackServerForTxn(CoordinatorTransaction transaction, Integer serverId) throws CrashException {
        transaction.getServers().add(servers.get(serverId));
        maybeCrash(CrashBefore2PC.ON_CLIENT_MSG);
    }

    public void onWelcomeMsg(Message.WelcomeMsg msg) {                   /* Start */
        setGroup(msg);
    }

    public void onTxnBeginMsg(ClientCoordinatorMessage.TxnBeginMsg msg) {
        // initialize transaction
        CoordinatorTransaction t = client2transaction.get(getSender());
        try {
            // if were already managing a transaction from this client, abort it and start a new one
            if (t != null)
                takeDecision(t, CoordinatorServerMessage.Decision.ABORT);

            t = new CoordinatorTransaction(msg.clientId, msg.numAttemptedTxn, getSender());
            client2transaction.put(getSender(), t);
            transaction2client.put(t, getSender());
            pendingTransactions.add(t);
            if (Main.COORD_DEBUG_BEGIN_TXN)
                print("GOT TXN BEGIN FROM " + msg.clientId + ", NEW_TXN: " + msg.numAttemptedTxn + ", OLD_TXN: " + (t != null ? t.getTxnId() : "null"));
            // send accept
            maybeCrash(CrashBefore2PC.BEFORE_TXN_ACCEPT_MSG);
            reply(new ClientCoordinatorMessage.TxnAcceptMsg(msg.clientId, msg.numAttemptedTxn));
        } catch (CrashException ignored) {
        }
    }

    public void onTxnEndMsg(ClientCoordinatorMessage.TxnEndMsg msg) {
        CoordinatorTransaction transaction = client2transaction.get(getSender());
        if (isCurrentTransaction(transaction, msg)) {
            try {
                if (msg.commit) {
                    if (Main.COORD_DEBUG_BEGIN_VOTE)
                        print("SENDING VOTE REQUEST");
                    // send vote request
                    multicast(new CoordinatorServerMessage.VoteRequest(transaction, transaction.getServers()), transaction.getServers(),
                            true, CrashDuring2PC.CrashDuringVote.class);
                    transaction.setState(Transaction.State.READY);
                } else {
                    takeDecision(transaction, CoordinatorServerMessage.Decision.ABORT);
                }
            } catch (CrashException ignored) {
            }
        }
    }

    public void onReadMsg(ClientCoordinatorMessage.ReadMsg msg) {
        CoordinatorTransaction transaction = client2transaction.get(getSender());
        // ignored messages after the transaction is decided
        if (!hasDecided(transaction)) {
            // ignore messages if they refer to an old transaction
            if (isCurrentTransaction(transaction, msg)) {
                if (Main.COORD_DEBUG_READ) {
                    print("READING KEY " + msg.key);
                }
                int key = msg.key;
                int serverId = key / Server.DB_SIZE;
                try {
                    trackServerForTxn(transaction, serverId);
                    sendMessage(servers.get(serverId), new CoordinatorServerMessage.TransactionRead(transaction, key), true);
                } catch (CrashException ignored) {
                }
            }
        }
    }

    public void onTxnReadResponseMsg(CoordinatorServerMessage.TxnReadResponseMsg msg) {
        if (!hasDecided(msg.transaction)) {
            unsetTimeout(msg.transaction, getSender());
            ActorRef c = transaction2client.get(msg.transaction);
            try {
                maybeCrash(CrashBefore2PC.ON_SERVER_MSG);
                // forward answer to the client
                sendMessage(c, new ClientCoordinatorMessage.ReadResultMsg(
                        msg.transaction.getClientId(),
                        msg.transaction.getNumAttemptedTxn(),
                        msg.key,
                        msg.valueRead));
                if (Main.COORD_DEBUG_READ_RESPONSE)
                    print("REPLYING WITH VALUE " + msg.valueRead + " FOR KEY " + msg.key);
            } catch (CrashException e) {
            }
        }

    }

    public void onWriteMsg(ClientCoordinatorMessage.WriteMsg msg) {
        CoordinatorTransaction transaction = client2transaction.get(getSender());
        if (!hasDecided(transaction)) {
            if (isCurrentTransaction(transaction, msg)) {
                int key = msg.key;
                int value = msg.value;
                int serverId = key / Server.DB_SIZE;
                try {
                    trackServerForTxn(transaction, serverId);
                    sendMessage(servers.get(serverId), new CoordinatorServerMessage.TransactionWrite(transaction, key, value));
                } catch (CrashException e) {
                }
            }
        }
    }


    public void onVoteResponse(CoordinatorServerMessage.VoteResponse msg) {                    /* Vote */
        if (hasDecided(msg.transaction)) {
            // we have already decided and sent the decision to the group,
            // so do not care about other votes.
            // Typically, this happens when we receive an abort vote and we immediately decide abort,
            // while other cohorts may have not voted yet.
            return;
        }

        CoordinatorTransaction transaction = getCTfromTransaction(msg.transaction);
        CoordinatorServerMessage.Vote v = (msg).vote;
        if (Main.COORD_DEBUG_RECEIVED_VOTE)
            print("RECEIVED VOTE " + v.toString() + " FROM SERVER " + servers.indexOf(getSender()));
        if (v == CoordinatorServerMessage.Vote.YES) {
            transaction.getYesVoters().add(getSender());
            if (allVotedYes(transaction)) {
                if (Main.COORD_DEBUG_ALL_VOTED_YES)
                    print("ALL VOTED YES");
                try {
                    takeDecision(transaction, CoordinatorServerMessage.Decision.COMMIT);
                } catch (CrashException ignored) {
                }
            }
        } else { // a NO vote
            // on a single NO we decide ABORT
            try {
                takeDecision(transaction, CoordinatorServerMessage.Decision.ABORT);
            } catch (CrashException e) {
            }
        }
    }

    public void onTimeoutMsg(CoordinatorServerMessage.TimeoutMsg msg) {
        if (Main.COORD_DEBUG_TIMEOUT)
            print("TIMEOUT FOR TRANSACTION " + msg.transaction.getTxnId());
        CoordinatorTransaction t = getCTfromTransaction(msg.transaction);
        if (t == null)
            return;
        unsetTimeout(t);

        // if in INIT -> server crashed before 2PC or while sending the vote request
        // if in READY -> at least one server did not respond to the vote request (and nobody voted abort)
        assert t.getState() != Transaction.State.DECIDED;
        try {
            takeDecision(t, CoordinatorServerMessage.Decision.ABORT);
        } catch (CrashException ignored) {
        }
    }

    @Override
    public void onRecoveryMsg(CoordinatorServerMessage.RecoveryMsg msg) {
        getContext().become(createReceive());

        if (Main.COORD_DEBUG_RECOVERY)
            print("RECOVERED");

        try {
            // abort every pending transaction (not decided yet)
            for (Transaction t : new HashSet<>(pendingTransactions)) {
                takeDecision(t, CoordinatorServerMessage.Decision.ABORT);
            }
        } catch (CrashException e) {
        }
    }

    @Override
    public void onCheckCorrectness(Message.CheckCorrectness msg) {
        reply(new Message.CheckCorrectnessResponse(id, null, numCrashes));
        getContext().stop(getSelf());
    }
}
