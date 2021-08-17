package it.unitn.ds1.actors;

import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.ds1.transactions.Transaction;
import it.unitn.ds1.messages.CoordinatorServerMessage;
import it.unitn.ds1.messages.Message;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Set;


/*-- Participant -----------------------------------------------------------*/
public class Checker extends Node {
    private Integer numServers;
    private Integer counter;
    private Integer partialSum;
    public Checker(int numServers, Set<CrashPhase> crashPhases){
        super(-1, crashPhases);
        this.numServers = numServers;
        this.counter = 0;
        this.partialSum = 0;
    }

    @Override
    protected void onRecovery(CoordinatorServerMessage.Recovery msg) {

    }

    @Override
    void multicastAndCrash(Serializable m, int recoverIn){

    }

    @Override
    void fixDecision(Transaction transaction, CoordinatorServerMessage.Decision d) {

    }

    static public Props props(int numServers, Set<CrashPhase> crashPhases) { return Props.create(Checker.class, () -> new Checker(numServers, crashPhases)); }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Message.WelcomeMsg.class, this::onWelcomeMsg)
                .match(Message.CheckCorrectness.class, this::onCheckCorrectness)
                .match(Message.CheckCorrectnessResponse.class, this::onCheckCorrectnessResponse)
                .build();
    }

    public void onWelcomeMsg(Message.WelcomeMsg msg) {                   /* Start */
        setGroup(msg);
    }

    public void onCheckCorrectness(Message.CheckCorrectness msg) {
        for(ActorRef server : servers){
            server.tell(new Message.CheckCorrectness(), getSelf());
        }
    }

    public void onCheckCorrectnessResponse (Message.CheckCorrectnessResponse msg) throws InterruptedException {
        counter++;
        partialSum += msg.value;
        if(counter == numServers){
            Thread.sleep(3000);
            System.out.println("##### CORRECTNESS CHECK #####");
            Integer correctSum = numServers * (Server.DB_SIZE * Server.DEFAULT_VALUE);
            System.out.println("CORRECT SUM: " + correctSum);
            System.out.println("ACTUAL SUM: " + partialSum);
            assert(partialSum == correctSum);
            System.out.println("##### CORRECTNESS CHECK #####");
        }
    }

}