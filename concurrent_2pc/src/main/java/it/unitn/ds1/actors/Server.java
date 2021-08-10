package it.unitn.ds1.actors;

import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.ds1.Main;
import it.unitn.ds1.Resource;
import it.unitn.ds1.Transaction;
import it.unitn.ds1.messages.CoordinatorServerMessages;
import it.unitn.ds1.messages.Message;

import java.util.*;


/*-- Participant -----------------------------------------------------------*/
public class Server extends Node {
    public static final Integer DEFAULT_VALUE = 100;
    public static final Integer DB_SIZE = 10;
    private final Map<Integer, Resource> database;
    private final Map<Transaction, Map<Integer, Map.Entry<Resource, Boolean>>> workspaces = new HashMap<>();
    private final Set<Integer> pendingResource = new HashSet<>();
    private final Map<Transaction, ActorRef> transaction2coordinator = new HashMap<>();

    public Server(int id) {
        super(id);
        database = new HashMap<>();
        for (int i = id * DB_SIZE; i < (id + 1) * DB_SIZE; i++)
            database.put(i, new Resource(DEFAULT_VALUE, 0));
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
                .match(Message.CheckCorrectness.class, this::onCheckCorrectness)
                .build();
    }

    public void onWelcomeMsg(Message.WelcomeMsg msg) {
        setGroup(msg);
    }

    private Boolean canCommit(Transaction transaction){
        Map<Integer, Map.Entry<Resource, Boolean>> workspace = workspaces.get(transaction);

        for(Map.Entry<Integer, Map.Entry<Resource, Boolean>> entry : workspace.entrySet()) {
            Integer version = entry.getValue().getKey().getVersion();
            Integer key = entry.getKey();
            if(version != database.get(key).getVersion() || pendingResource.contains(key)) {
                return false;
            }
        }
        return true;
    }



    private void freeWorkspace(Transaction transaction){
        freePendingResources(transaction);
        workspaces.remove(transaction);
        transaction2coordinator.remove(transaction);
    }

    private void lockResources(Transaction transaction){
        Map<Integer, Map.Entry<Resource, Boolean>> workspace = workspaces.get(transaction);
        for(Integer key : workspace.keySet()) {
            pendingResource.add(key);
        }
    }

    private void freePendingResources(Transaction transaction){
        Map<Integer, Map.Entry<Resource, Boolean>> workspace = workspaces.get(transaction);
        for(Integer key : workspace.keySet()) {
            pendingResource.remove(key);
        }
    }

    public void onVoteRequest(CoordinatorServerMessages.VoteRequest msg) {
        Transaction transaction = msg.transaction;
        CoordinatorServerMessages.Vote vote = null;
        //if (id==2) {crash(5000); return;}    // simulate a crash
        //if (id==2) delay(4000);              // simulate a delay
        if(!canCommit(transaction)){
            fixDecision(transaction, CoordinatorServerMessages.Decision.ABORT);
            vote = CoordinatorServerMessages.Vote.NO;
        } else {
            transaction2coordinator.put(transaction, getSender());
            lockResources(transaction);
            vote = CoordinatorServerMessages.Vote.YES;
        }

        print("Server " + id + " sending vote " + vote);
        getSender().tell(new CoordinatorServerMessages.VoteResponse(transaction, vote), getSelf());
//        setTimeout(Main.DECISION_TIMEOUT);
    }

    public void onTimeout(CoordinatorServerMessages.Timeout msg) {
//        if (!hasDecided()) {
//            print("Timeout. Asking around.");
//
//            // TODO 3: participant termination protocol
//            // termination protocol: ask group if anybody knows the decision
//        }
    }

    @Override
    public void onRecovery(CoordinatorServerMessages.Recovery msg) {
//        getContext().become(createReceive());
//
//        // We don't handle explicitly the "not voted" case here
//        // (in any case, it does not break the protocol)
//        if (!hasDecided()) {
//            print("Recovery. Asking the coordinator.");
//            coordinator.tell(new CoordinatorServerMessages.DecisionRequest(), getSelf());
//            setTimeout(Main.DECISION_TIMEOUT);
//        }
    }

    private void commitWorkspace(Transaction transaction){
        Map<Integer, Map.Entry<Resource, Boolean>> workspace = workspaces.get(transaction);

        for(Map.Entry<Integer, Map.Entry<Resource, Boolean>> entry : workspace.entrySet()) {
            Integer key = entry.getKey();
            Integer value = entry.getValue().getKey().getValue();
            Integer version = entry.getValue().getKey().getVersion();
            Boolean changed = entry.getValue().getValue();

            assert(version == database.get(key).getVersion());
            if(changed) {
                database.get(key).setValue(value);
                database.get(key).setVersion(version + 1);
            }
        }
    }

    @Override
    void fixDecision(Transaction transaction, CoordinatorServerMessages.Decision d) {
        if (!hasDecided(transaction)) {
            transaction2decision.put(transaction, d);
            print("decided " + d);
            if(d == CoordinatorServerMessages.Decision.COMMIT){
                commitWorkspace(transaction);
            }
            freeWorkspace(transaction);
        }

    }

    public void onDecisionResponse(CoordinatorServerMessages.DecisionResponse msg) { /* Decision Response */
        Transaction transaction = msg.transaction;
        // store the decision
        fixDecision(transaction, msg.decision);
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
        getSender().tell(new CoordinatorServerMessages.TxnReadResponseMsg(msg.transaction, msg.key, valueRead),
                getSelf());
    }


    public void onTransactionWrite(CoordinatorServerMessages.TransactionWrite msg) {
        Map.Entry<Resource, Boolean> resource = processWorkspace(msg);
        resource.getKey().setValue(msg.value);
        // changed
        resource.setValue(true);
//        getSender().tell(new CoordinatorServerMessages.TransactionReadResponse(valueRead), getSelf());
    }

    public void onCheckCorrectness(Message.CheckCorrectness msg){
        Integer result = 0;
        for (Integer key: database.keySet()) {
            result += database.get(key).getValue();
        }
        getSender().tell(new Message.CheckCorrectnessResponse(result), getSelf());
    }

}