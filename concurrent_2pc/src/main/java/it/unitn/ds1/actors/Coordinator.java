package it.unitn.ds1.actors;

/*-- Coordinator -----------------------------------------------------------*/

import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.ds1.Main;
import it.unitn.ds1.Transaction;
import it.unitn.ds1.messages.ClientCoordinatorMessages;
import it.unitn.ds1.messages.CoordinatorServerMessages;
import it.unitn.ds1.messages.Message;

import java.util.*;

public class Coordinator extends Node {

    // here all the nodes that sent YES are collected
    private final Set<ActorRef> yesVoters = new HashSet<>();
    private final Map<ActorRef, Transaction> client2transaction = new HashMap<>();
    // TODO: refactor
    private final Map<Transaction, ActorRef> transaction2client = new HashMap<>();
    private final Map<Transaction, Set<ActorRef>> transaction2servers = new HashMap<>();
    private final Map<Transaction, Set<ActorRef>> transaction2yesVoters = new HashMap<>();

    boolean allVotedYes(Transaction transaction) { // returns true if all voted YES
        return transaction2yesVoters.get(transaction).size() == transaction2servers.get(transaction).size();
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
            print("decided " + d);
        }
    }

    public void onVoteResponse(CoordinatorServerMessages.VoteResponse msg) {                    /* Vote */
        Transaction transaction = msg.transaction;
        if (hasDecided(transaction)) {
            // we have already decided and sent the decision to the group,
            // so do not care about other votes
            return;
        }
        CoordinatorServerMessages.Vote v = (msg).vote;
        if (v == CoordinatorServerMessages.Vote.YES) {
            transaction2yesVoters.get(transaction).add(getSender());
            if (allVotedYes(transaction)) {
                fixDecision(transaction, CoordinatorServerMessages.Decision.COMMIT);
                //if (id==-1) {crash(3000); return;}
                multicast(new CoordinatorServerMessages.DecisionResponse(transaction, transaction2decision.get(transaction)), transaction2servers.get(transaction));
//                multicastAndCrash(new CoordinatorServerMessages.DecisionResponse(transaction2decision.get(transaction)), 3000);
            }
        } else { // a NO vote

            // on a single NO we decide ABORT
            fixDecision(transaction, CoordinatorServerMessages.Decision.ABORT);
            multicast(new CoordinatorServerMessages.DecisionResponse(transaction, transaction2decision.get(transaction)), transaction2servers.get(transaction));
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
        Transaction t = new Transaction(msg.clientId, msg.numAttemptedTxn);
        client2transaction.put(getSender(), t);
        transaction2client.put(t, getSender());
        // send accept
        getSender().tell(new ClientCoordinatorMessages.TxnAcceptMsg(), getSelf());
    }

    public void onTxnEndMsg(ClientCoordinatorMessages.TxnEndMsg msg) {
        Transaction transaction = client2transaction.get(getSender());
        Set<ActorRef> servers = transaction2servers.get(transaction);
        transaction2yesVoters.put(transaction, new HashSet<>());

        multicast(new CoordinatorServerMessages.VoteRequest(transaction), servers);
//        print("Sending vote requests");
//        multicast(new CoordinatorServerMessages.VoteRequest());
//      multicastAndCrash(new VoteRequest(), 3000);
//        setTimeout(Main.VOTE_TIMEOUT);
//      crash(5000);
    }

    private void trackServerForTxn(Transaction transaction, Integer serverId){
        if(!transaction2servers.containsKey(transaction)){
            transaction2servers.put(transaction, new HashSet<>());
        }
        transaction2servers.get(transaction).add(servers.get(serverId));
    }

    public void onReadMsg (ClientCoordinatorMessages.ReadMsg msg) {
        int key = msg.key;
        int serverId = key / Server.DB_SIZE;
        Transaction transaction = client2transaction.get(getSender());
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
        Transaction transaction = client2transaction.get(getSender());
        trackServerForTxn(transaction, serverId);
        servers.get(serverId).tell(new CoordinatorServerMessages.TransactionWrite(transaction, key, value), getSelf());
    }
}