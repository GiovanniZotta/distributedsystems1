package it.unitn.ds1.transactions;

import akka.actor.ActorRef;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class CoordinatorTransaction extends Transaction {
    private final ActorRef client;
    private final Set<ActorRef> servers;
    private final Set<ActorRef> yesVoters;

    public CoordinatorTransaction(Integer clientId, Integer numAttemptedTxn, ActorRef client) {
        super(clientId, numAttemptedTxn);
        this.client = client;
        this.servers = new HashSet<>();
        this.yesVoters = new HashSet<>();
    }

    public ActorRef getClient() {
        return client;
    }

    public Set<ActorRef> getServers() {
        return servers;
    }

    public Set<ActorRef> getYesVoters() {
        return yesVoters;
    }
}
