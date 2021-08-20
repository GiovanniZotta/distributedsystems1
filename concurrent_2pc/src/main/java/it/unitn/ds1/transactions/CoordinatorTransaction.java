package it.unitn.ds1.transactions;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import scala.collection.mutable.MultiMap;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class CoordinatorTransaction extends Transaction {
    private final ActorRef client;
    private final Set<ActorRef> servers;
    private final Set<ActorRef> yesVoters;
    private final Map<ActorRef, Queue<Cancellable>> timeouts;

    public CoordinatorTransaction(Integer clientId, Integer numAttemptedTxn, ActorRef client) {
        super(clientId, numAttemptedTxn);
        this.client = client;
        this.servers = new HashSet<>();
        this.yesVoters = new HashSet<>();
        timeouts = new HashMap<>();
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

    public Cancellable popOldestServerTimeout(ActorRef server) {
        return timeouts.get(server).remove();
    }

    public void pushServerTimeout(ActorRef server, Cancellable timeout) {
        if (!timeouts.containsKey(server))
            timeouts.put(server, new LinkedBlockingQueue<Cancellable>());
        timeouts.get(server).add(timeout);
    }

    public Boolean hasTimeout(ActorRef server) {
        return timeouts.containsKey(server) && !timeouts.get(server).isEmpty();
    }
}
