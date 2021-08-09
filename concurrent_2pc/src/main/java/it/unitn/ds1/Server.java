package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.Props;


/*-- Participant -----------------------------------------------------------*/
public class Server extends Node {
    ActorRef coordinator;

    public Server(int id) {
        super(id);
    }

    static public Props props(int id) {
        return Props.create(Server.class, () -> new Server(id));
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(CoordinatorServerMessages.StartMessage.class, this::onStartMessage)
                .match(CoordinatorServerMessages.VoteRequest.class, this::onVoteRequest)
                .match(CoordinatorServerMessages.DecisionRequest.class, this::onDecisionRequest)
                .match(CoordinatorServerMessages.DecisionResponse.class, this::onDecisionResponse)
                .match(CoordinatorServerMessages.Timeout.class, this::onTimeout)
                .match(CoordinatorServerMessages.Recovery.class, this::onRecovery)
                .match(CoordinatorServerMessages.TransactionRead.class, this::onTransactionRead)
                .build();
    }

    public void onStartMessage(CoordinatorServerMessages.StartMessage msg) {
        setGroup(msg);
    }

    public void onVoteRequest(CoordinatorServerMessages.VoteRequest msg) {
        this.coordinator = getSender();
        //if (id==2) {crash(5000); return;}    // simulate a crash
        //if (id==2) delay(4000);              // simulate a delay
        if (Main.predefinedVotes[this.id] == CoordinatorServerMessages.Vote.NO) {
            fixDecision(CoordinatorServerMessages.Decision.ABORT);
        }
        print("sending vote " + Main.predefinedVotes[this.id]);
        this.coordinator.tell(new CoordinatorServerMessages.VoteResponse(Main.predefinedVotes[this.id]), getSelf());
        setTimeout(Main.DECISION_TIMEOUT);
    }

    public void onTimeout(CoordinatorServerMessages.Timeout msg) {
        if (!hasDecided()) {
            print("Timeout. Asking around.");

            // TODO 3: participant termination protocol
            // termination protocol: ask group if anybody knows the decision
        }
    }

    @Override
    public void onRecovery(CoordinatorServerMessages.Recovery msg) {
        getContext().become(createReceive());

        // We don't handle explicitly the "not voted" case here
        // (in any case, it does not break the protocol)
        if (!hasDecided()) {
            print("Recovery. Asking the coordinator.");
            coordinator.tell(new CoordinatorServerMessages.DecisionRequest(), getSelf());
            setTimeout(Main.DECISION_TIMEOUT);
        }
    }

    public void onDecisionResponse(CoordinatorServerMessages.DecisionResponse msg) { /* Decision Response */

        // store the decision
        fixDecision(msg.decision);
    }

    public void onTransactionRead(CoordinatorServerMessages.TransactionRead msg) {
        // TODO
    }
}