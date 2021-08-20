package it.unitn.ds1.transactions;

import akka.actor.ActorRef;
import akka.actor.Cancellable;

import java.util.List;

public class ServerTransaction extends Transaction {

    private final Workspace workspace;
    private ActorRef coordinator;
    private List<ActorRef> servers;
    private Cancellable timeout;

    public ServerTransaction(Integer clientId, Integer numAttemptedTxn, ActorRef coordinator) {
        super(clientId, numAttemptedTxn);
        this.workspace = new Workspace();
        this.coordinator = coordinator;
        this.servers = null;
    }

    public Workspace getWorkspace() {
        return workspace;
    }

    public ActorRef getCoordinator() {
        return coordinator;
    }

    public void setCoordinator(ActorRef coordinator) {
        this.coordinator = coordinator;
    }

    public List<ActorRef> getServers() {
        return servers;
    }

    public void setServers(List<ActorRef> servers) {
        this.servers = servers;
    }

    public Cancellable getTimeout() {
        return timeout;
    }

    public void setTimeout(Cancellable timeout) {
        this.timeout = timeout;
    }
}
