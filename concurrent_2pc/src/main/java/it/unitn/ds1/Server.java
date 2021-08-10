package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.ds1.messages.ClientCoordinatorMessages;
import it.unitn.ds1.messages.CoordinatorServerMessages;
import it.unitn.ds1.messages.Message;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;


/*-- Participant -----------------------------------------------------------*/
public class Server extends Node {
    public static final Integer DEFAULT_VALUE = 100;
    public static final Integer DB_SIZE = 10;
    private ActorRef coordinator;
    private final Map<Integer, Resource> database;
    private final Map<Transaction, Map<Integer, Map.Entry<Resource, Boolean>>> workspaces;

    public Server(int id) {
        super(id);
        database = new HashMap<>();
        for (int i = id * DB_SIZE; i < (id + 1) * DB_SIZE; i++)
            database.put(i, new Resource(DEFAULT_VALUE, 0));
        workspaces = new HashMap<>();
    }

    static public Props props(int id) {
        return Props.create(Server.class, () -> new Server(id));
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Message.WelcomeMsg.class, this::onWelcomeMsg)
                .match(CoordinatorServerMessages.VoteRequest.class, this::onVoteRequest)
                .match(CoordinatorServerMessages.DecisionRequest.class, this::onDecisionRequest)
                .match(CoordinatorServerMessages.DecisionResponse.class, this::onDecisionResponse)
                .match(CoordinatorServerMessages.Timeout.class, this::onTimeout)
                .match(CoordinatorServerMessages.Recovery.class, this::onRecovery)
                .match(CoordinatorServerMessages.TransactionRead.class, this::onTransactionRead)
                .match(CoordinatorServerMessages.TransactionWrite.class, this::onTransactionWrite)
                .build();
    }

    public void onWelcomeMsg(Message.WelcomeMsg msg) {
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

    private Map.Entry<Resource, Boolean> processWorkspace(CoordinatorServerMessages.TransactionAction msg) {
        // create workspace if the transaction is new
        if (!workspaces.containsKey(msg.transaction)) {
            workspaces.put(msg.transaction, new HashMap<>());
        }

        Map<Integer, Map.Entry<Resource, Boolean>> ws = workspaces.get(msg.transaction);
        if (!ws.containsKey(msg.key)) {
            Resource r = (Resource) database.get(msg.key).clone();
            ws.put(msg.key, new AbstractMap.SimpleEntry<>(r, false));
        }
        return ws.get(msg.key);
    }

    public void onTransactionRead(CoordinatorServerMessages.TransactionRead msg) {
        int valueRead = processWorkspace(msg).getKey().getValue();
        getSender().tell(new CoordinatorServerMessages.TransactionReadResponse(msg.transaction, msg.key, valueRead),
                getSelf());
    }


    public void onTransactionWrite(CoordinatorServerMessages.TransactionWrite msg) {
        Map.Entry<Resource, Boolean> t = processWorkspace(msg);
        t.getKey().setValue(msg.value);
        t.setValue(true);
//        getSender().tell(new CoordinatorServerMessages.TransactionReadResponse(valueRead), getSelf());
    }

}