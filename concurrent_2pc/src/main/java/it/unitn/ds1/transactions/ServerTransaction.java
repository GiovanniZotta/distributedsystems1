package it.unitn.ds1.transactions;

import akka.actor.ActorRef;

public class ServerTransaction extends Transaction {

    private final Workspace workspace;
    private ActorRef coordinator;

    public ServerTransaction(Integer clientId, Integer numAttemptedTxn, ActorRef coordinator) {
        super(clientId, numAttemptedTxn);
        this.workspace = new Workspace();
        this.coordinator = coordinator;
    }

    public Workspace getWorkspace() {
        return workspace;
    }

    public ActorRef getCoordinator() {
        return coordinator;
    }


}
