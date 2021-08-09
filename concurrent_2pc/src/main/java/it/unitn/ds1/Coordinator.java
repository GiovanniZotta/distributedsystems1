package it.unitn.ds1;

/*-- Coordinator -----------------------------------------------------------*/

import akka.actor.ActorRef;
import akka.actor.Props;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Coordinator extends Node {

    // here all the nodes that sent YES are collected
    private final Set<ActorRef> yesVoters = new HashSet<>();
    private final Map<ActorRef, Transaction> transactionMap = new HashMap<>();

    boolean allVotedYes() { // returns true if all voted YES
        return yesVoters.size() >= Main.N_SERVER;
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
                .match(CoordinatorServerMessages.StartMessage.class, this::onStartMessage)
                .match(CoordinatorServerMessages.VoteResponse.class, this::onVoteResponse)
                .match(CoordinatorServerMessages.Timeout.class, this::onTimeout)
                .match(CoordinatorServerMessages.DecisionRequest.class, this::onDecisionRequest)
                .match(ClientCoordinatorMessages.TxnBeginMsg.class, this::onTxnBeginMsg)
                .match(ClientCoordinatorMessages.ReadMsg.class, this::onReadMsg)
                .build();
    }

    public void onStartMessage(CoordinatorServerMessages.StartMessage msg) {                   /* Start */
        setGroup(msg);
        print("Sending vote request");
        multicast(new CoordinatorServerMessages.VoteRequest());
        //multicastAndCrash(new VoteRequest(), 3000);
        setTimeout(Main.VOTE_TIMEOUT);
        //crash(5000);
    }

    public void onVoteResponse(CoordinatorServerMessages.VoteResponse msg) {                    /* Vote */
        if (hasDecided()) {

            // we have already decided and sent the decision to the group,
            // so do not care about other votes
            return;
        }
        CoordinatorServerMessages.Vote v = (msg).vote;
        if (v == CoordinatorServerMessages.Vote.YES) {
            yesVoters.add(getSender());
            if (allVotedYes()) {
                fixDecision(CoordinatorServerMessages.Decision.COMMIT);
                //if (id==-1) {crash(3000); return;}
                //multicast(new DecisionResponse(decision));
                multicastAndCrash(new CoordinatorServerMessages.DecisionResponse(decision), 3000);
            }
        } else { // a NO vote

            // on a single NO we decide ABORT
            fixDecision(CoordinatorServerMessages.Decision.ABORT);
            multicast(new CoordinatorServerMessages.DecisionResponse(decision));
        }
    }

    public void onTimeout(CoordinatorServerMessages.Timeout msg) {
        if (!hasDecided()) {
            print("Timeout");

            // TODO 1: coordinator timeout action
            // abort to avoid blocking
        }
    }

    @Override
    public void onRecovery(CoordinatorServerMessages.Recovery msg) {
        getContext().become(createReceive());

        // TODO 2: coordinator recovery action
    }

    public void onTxnBeginMsg(ClientCoordinatorMessages.TxnBeginMsg msg) {
        // initialize transaction
        transactionMap.put(getSender(), new Transaction(msg.clientId, msg.numAttemptedTxn));
        // send accept
        getSender().tell(new ClientCoordinatorMessages.TxnAcceptMsg(), getSelf());
    }

    public void onReadMsg (ClientCoordinatorMessages.ReadMsg msg) {
        int key = msg.key;
        int serverId = key % 10;
        Transaction transaction = transactionMap.get(getSender());
        servers.get(serverId).tell(new CoordinatorServerMessages.TransactionRead(transaction, key), getSelf());
    }
}