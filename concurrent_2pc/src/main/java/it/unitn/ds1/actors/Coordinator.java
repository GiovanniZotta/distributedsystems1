package it.unitn.ds1.actors;

/*-- Coordinator -----------------------------------------------------------*/

import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.ds1.Main;
import it.unitn.ds1.transactions.CoordinatorTransaction;
import it.unitn.ds1.transactions.ServerTransaction;
import it.unitn.ds1.transactions.Transaction;
import it.unitn.ds1.messages.ClientCoordinatorMessages;
import it.unitn.ds1.messages.CoordinatorServerMessages;
import it.unitn.ds1.messages.Message;

import java.util.*;

public class Coordinator extends Node {

    // here all the nodes that sent YES are collected
    private final Map<ActorRef, CoordinatorTransaction> client2transaction = new HashMap<>();
    private final Map<Transaction, ActorRef> transaction2client = new HashMap<>();

    boolean allVotedYes(CoordinatorTransaction transaction) { // returns true if all voted YES
        return transaction.getYesVoters().size() == transaction.getServers().size();
    }

    public Coordinator(int id) {
        super(id);
    }

    static public Props props(int id) {
        return Props.create(Coordinator.class, () -> new Coordinator(id));
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(CoordinatorServerMessages.Recovery.class, this::onRecovery)
                .match(Message.WelcomeMsg.class, this::onWelcomeMsg)
                .match(CoordinatorServerMessages.VoteResponse.class, this::onVoteResponse)
                .match(CoordinatorServerMessages.Timeout.class, this::onTimeout)
                .match(CoordinatorServerMessages.DecisionRequest.class, this::onDecisionRequest)
                .match(ClientCoordinatorMessages.TxnBeginMsg.class, this::onTxnBeginMsg)
                .match(ClientCoordinatorMessages.TxnEndMsg.class, this::onTxnEndMsg)
                .match(ClientCoordinatorMessages.ReadMsg.class, this::onReadMsg)
                .match(CoordinatorServerMessages.TxnReadResponseMsg.class, this::onTxnReadResponseMsg)
                .match(ClientCoordinatorMessages.WriteMsg.class, this::onWriteMsg)
                .build();
    }

    public void onWelcomeMsg(Message.WelcomeMsg msg) {                   /* Start */
        setGroup(msg);
    }

    boolean hasDecided(Transaction transaction) {
        return transaction2decision.get(transaction) != null;
    }

    // fix the final decision of the current node
    @Override
    void fixDecision(Transaction transaction, CoordinatorServerMessages.Decision d) {
        if (!hasDecided(transaction)) {
            transaction2decision.put(transaction, d);
            transaction2client.get(transaction).tell(new ClientCoordinatorMessages.TxnResultMsg(d == CoordinatorServerMessages.Decision.COMMIT), getSelf());
            if(Main.COORD_DEBUG_DECISION)
                print("Coordinator " + this.id + " decided " + d
                + " on transaction " + transaction.getTxnId());
        }
    }

    private CoordinatorTransaction getSTfromTransaction(Transaction transaction){
        ActorRef c = transaction2client.get(transaction);
        return client2transaction.get(c);
    }

    public void onVoteResponse(CoordinatorServerMessages.VoteResponse msg) {                    /* Vote */
        CoordinatorTransaction transaction = getSTfromTransaction(msg.transaction);
        if (hasDecided(transaction)) {
            // we have already decided and sent the decision to the group,
            // so do not care about other votes
            return;
        }
        CoordinatorServerMessages.Vote v = (msg).vote;
        if (v == CoordinatorServerMessages.Vote.YES) {
            transaction.getYesVoters().add(getSender());
            if (allVotedYes(transaction)) {
                takeDecision(transaction, CoordinatorServerMessages.Decision.COMMIT);
                //if (id==-1) {crash(3000); return;}
//                multicastAndCrash(new CoordinatorServerMessages.DecisionResponse(transaction2decision.get(transaction)), 3000);
            }
        } else { // a NO vote
            // on a single NO we decide ABORT
            takeDecision(transaction, CoordinatorServerMessages.Decision.ABORT);
        }
    }

    public void onTimeout(CoordinatorServerMessages.Timeout msg) {
//        if (!hasDecided(tra)) {
//            print("Timeout");
//
//            // TODO 1: coordinator timeout action
//            // abort to avoid blocking
//        }
    }

    @Override
    public void onRecovery(CoordinatorServerMessages.Recovery msg) {
        getContext().become(createReceive());

        // TODO 2: coordinator recovery action
    }

    public void onTxnBeginMsg(ClientCoordinatorMessages.TxnBeginMsg msg) {
        // initialize transaction
        CoordinatorTransaction t = new CoordinatorTransaction(msg.clientId, msg.numAttemptedTxn, getSender());
        client2transaction.put(getSender(), t);
        transaction2client.put(t, getSender());
        // send accept
        getSender().tell(new ClientCoordinatorMessages.TxnAcceptMsg(), getSelf());
    }

    private void takeDecision(Transaction transaction, CoordinatorServerMessages.Decision decision){
        CoordinatorTransaction transaction1 = getSTfromTransaction(transaction);
        fixDecision(transaction1, decision);
        multicast(new CoordinatorServerMessages.DecisionResponse(transaction1, transaction2decision.get(transaction1)), transaction1.getServers());
    }

    public void onTxnEndMsg(ClientCoordinatorMessages.TxnEndMsg msg) {
        CoordinatorTransaction transaction = client2transaction.get(getSender());
        if(msg.commit){
            multicast(new CoordinatorServerMessages.VoteRequest(transaction), transaction.getServers());
        } else {
            takeDecision(transaction, CoordinatorServerMessages.Decision.ABORT);
        }
//        print("Sending vote requests");
//        multicast(new CoordinatorServerMessages.VoteRequest());
//      multicastAndCrash(new VoteRequest(), 3000);
//        setTimeout(Main.VOTE_TIMEOUT);
//      crash(5000);
    }

    private void trackServerForTxn(CoordinatorTransaction transaction, Integer serverId){
        transaction.getServers().add(servers.get(serverId));
    }

    public void onReadMsg (ClientCoordinatorMessages.ReadMsg msg) {
        int key = msg.key;
        int serverId = key / Server.DB_SIZE;
        CoordinatorTransaction transaction = client2transaction.get(getSender());
        trackServerForTxn(transaction, serverId);
        servers.get(serverId).tell(new CoordinatorServerMessages.TransactionRead(transaction, key), getSelf());
    }

    public void onTxnReadResponseMsg(CoordinatorServerMessages.TxnReadResponseMsg msg) {
        ActorRef c = transaction2client.get(msg.transaction);
        c.tell(new ClientCoordinatorMessages.ReadResultMsg(msg.key, msg.valueRead), getSelf());
    }

    public void onWriteMsg (ClientCoordinatorMessages.WriteMsg msg) {
        int key = msg.key;
        int value = msg.value;
        int serverId = key / Server.DB_SIZE;
        CoordinatorTransaction transaction = client2transaction.get(getSender());
        trackServerForTxn(transaction, serverId);
        servers.get(serverId).tell(new CoordinatorServerMessages.TransactionWrite(transaction, key, value), getSelf());
    }
}