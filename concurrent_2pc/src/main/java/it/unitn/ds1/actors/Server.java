package it.unitn.ds1.actors;

import akka.actor.Props;
import it.unitn.ds1.Main;
import it.unitn.ds1.resources.Resource;
import it.unitn.ds1.resources.WorkspaceResource;
import it.unitn.ds1.transactions.ServerTransaction;
import it.unitn.ds1.transactions.Transaction;
import it.unitn.ds1.messages.CoordinatorServerMessage;
import it.unitn.ds1.messages.Message;
import it.unitn.ds1.transactions.Workspace;

import java.io.Serializable;
import java.util.*;


/*-- Participant -----------------------------------------------------------*/
public class Server extends Node {


    public enum CrashBefore2PC {
        ON_COORD_MSG;
        @Override
        public String toString() {
            return "ServerCrashBefore2PC_" + name();
        }
    }
    public static class CrashDuring2PC {
        public enum CrashDuringVote {
            NO_VOTE, AFTER_VOTE;
            @Override
            public String toString() {
                return Server.CrashDuring2PC.name() + "_CrashDuringVote_" + name();
            }
        }

        private static String name() {
            return "ServerCrashDuring2PC";
        }

        public enum CrashDuringTermination {
            NO_REPLY, RND_REPLY, ALL_REPLY;
            @Override
            public String toString() {
                return Server.CrashDuring2PC.name() + "_CrashDuringTermination_" + name();
            }
        }
    }

    public static final Integer DEFAULT_VALUE = 100;
    public static final Integer DB_SIZE = 10;
    private final Map<Integer, Resource> database;
    private final Map<Transaction, ServerTransaction> transactionMap = new HashMap<>();
    private final Map<Integer, Transaction> pendingResource = new HashMap<>();

    public Server(int id,  Set<Node.CrashPhase> crashPhases) {
        super(id, crashPhases);
        database = new HashMap<>();
        for (int i = id * DB_SIZE; i < (id + 1) * DB_SIZE; i++)
            database.put(i, new Resource(DEFAULT_VALUE, 0));
    }

    static public Props props(int id,  Set<CrashPhase> crashPhases) {
        return Props.create(Server.class, () -> new Server(id, crashPhases));
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Message.WelcomeMsg.class, this::onWelcomeMsg)
                .match(CoordinatorServerMessage.VoteRequest.class, this::onVoteRequest)
                .match(CoordinatorServerMessage.DecisionRequest.class, this::onDecisionRequest)
                .match(CoordinatorServerMessage.DecisionResponse.class, this::onDecisionResponse)
                .match(CoordinatorServerMessage.Timeout.class, this::onTimeout)
                .match(CoordinatorServerMessage.Recovery.class, this::onRecovery)
                .match(CoordinatorServerMessage.TransactionRead.class, this::onTransactionRead)
                .match(CoordinatorServerMessage.TransactionWrite.class, this::onTransactionWrite)
                .match(Message.CheckCorrectness.class, this::onCheckCorrectness)
                .build();
    }

    public void onWelcomeMsg(Message.WelcomeMsg msg) {
        setGroup(msg);
    }

    private Boolean canCommit(Transaction transaction){
        Workspace workspace = transactionMap.get(transaction).getWorkspace();

        for(Map.Entry<Integer, WorkspaceResource> entry : workspace.entrySet()) {
            Integer version = entry.getValue().getVersion();
            Integer key = entry.getKey();
            if(version != database.get(key).getVersion() || pendingResource.containsKey(key)) {
                return false;
            }
        }
        return true;
    }



    private void freeWorkspace(Transaction transaction){
        freePendingResources(transaction);
        transactionMap.remove(transaction);
//        transaction2coordinator.remove(transaction);
    }

    private void lockResources(Transaction transaction){
        Workspace workspace = transactionMap.get(transaction).getWorkspace();
        for(Integer key : workspace.keySet()){
            pendingResource.put(key, transaction);
        }
    }

    private void freePendingResources(Transaction transaction){
        Workspace workspace = transactionMap.get(transaction).getWorkspace();

        for(Integer key : workspace.keySet()) {
            if(pendingResource.get(key) != null && pendingResource.get(key).equals(transaction)) {
                pendingResource.remove(key);
            }
        }
    }

    public void onVoteRequest(CoordinatorServerMessage.VoteRequest msg) {
        Transaction transaction = msg.transaction;
        CoordinatorServerMessage.Vote vote = null;
        //if (id==2) {crash(5000); return;}    // simulate a crash
        //if (id==2) delay(4000);              // simulate a delay
        if(!canCommit(transaction)){
            fixDecision(transaction, CoordinatorServerMessage.Decision.ABORT);
            vote = CoordinatorServerMessage.Vote.NO;
        } else {
            lockResources(transaction);
            vote = CoordinatorServerMessage.Vote.YES;
        }
        if(Main.SERVER_DEBUG_SEND_VOTE)
            print("Server " + id + " sending vote " + vote);
        getSender().tell(new CoordinatorServerMessage.VoteResponse(transaction, vote), getSelf());
//        setTimeout(Main.DECISION_TIMEOUT);
    }

    public void onTimeout(CoordinatorServerMessage.Timeout msg) {
//        if (!hasDecided()) {
//            print("Timeout. Asking around.");
//
//            // TODO 3: participant termination protocol
//            // termination protocol: ask group if anybody knows the decision
//        }
    }

    @Override
    public void onRecovery(CoordinatorServerMessage.Recovery msg) {
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

    @Override
    void multicastAndCrash(Serializable m, int recoverIn) {

    }

    private void commitWorkspace(Transaction transaction){
        Workspace workspace = transactionMap.get(transaction).getWorkspace();

        for(Map.Entry<Integer, WorkspaceResource> entry : workspace.entrySet()) {
            Integer key = entry.getKey();
            Integer value = entry.getValue().getValue();
            Integer version = entry.getValue().getVersion();
            Boolean changed = entry.getValue().getChanged();

            assert(version.equals(database.get(key).getVersion()));
            if(changed) {
                database.get(key).setValue(value);
                database.get(key).setVersion(version + 1);
            }
        }
    }

    @Override
    void fixDecision(Transaction transaction, CoordinatorServerMessage.Decision d) {
        if (!hasDecided(transaction)) {
            transaction2decision.put(transaction, d);
            if(Main.SERVER_DEBUG_DECIDED)
                print(" Server decided " + d);
            if(d == CoordinatorServerMessage.Decision.COMMIT){
                commitWorkspace(transaction);
            }
            freeWorkspace(transaction);
        }

    }

    public void onDecisionResponse(CoordinatorServerMessage.DecisionResponse msg) { /* Decision Response */
        Transaction transaction = msg.transaction;
        // store the decision
        fixDecision(transaction, msg.decision);
    }

    private WorkspaceResource processWorkspace(CoordinatorServerMessage.TransactionAction msg) {
        // create workspace if the transaction is new
        if(!transactionMap.containsKey(msg.transaction)){
            Integer clientId = msg.transaction.getClientId();
            Integer clientAttemptedTxn = msg.transaction.getNumAttemptedTxn();
            ServerTransaction transaction = new ServerTransaction(clientId, clientAttemptedTxn, getSender());
            transactionMap.put(msg.transaction, transaction);
        }

        ServerTransaction transaction = transactionMap.get(msg.transaction);
        if(!transaction.getWorkspace().containsKey(msg.key)){
            Resource r = (Resource) database.get(msg.key).clone();
            transaction.getWorkspace().put(msg.key, new WorkspaceResource(r, false));
        }

        return transaction.getWorkspace().get(msg.key);
    }

    public void onTransactionRead(CoordinatorServerMessage.TransactionRead msg) {
        int valueRead = processWorkspace(msg).getValue();
        getSender().tell(new CoordinatorServerMessage.TxnReadResponseMsg(msg.transaction, msg.key, valueRead),
                getSelf());
    }


    public void onTransactionWrite(CoordinatorServerMessage.TransactionWrite msg) {
        WorkspaceResource resource = processWorkspace(msg);
        resource.setValue(msg.value);
        resource.setChanged(true);
//        getSender().tell(new CoordinatorServerMessages.TransactionReadResponse(valueRead), getSelf());
    }

    @Override
    public void onCheckCorrectness(Message.CheckCorrectness msg){
        Integer result = 0;
        for (Integer key: database.keySet()) {
            result += database.get(key).getValue();
        }
        getSender().tell(new Message.CheckCorrectnessResponse(id, result, numCrashes), getSelf());
    }

}