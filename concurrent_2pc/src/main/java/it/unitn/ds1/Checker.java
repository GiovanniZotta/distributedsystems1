package it.unitn.ds1;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.ds1.messages.ClientCoordinatorMessages;
import it.unitn.ds1.messages.CoordinatorServerMessages;
import it.unitn.ds1.messages.Message;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;


/*-- Participant -----------------------------------------------------------*/
public class Checker extends Node {
    private Integer numServers;
    private Integer counter;
    private Integer partialSum;
    public Checker(int numServers){
        super(-1);
        this.numServers = numServers;
        this.counter = 0;
        this.partialSum = 0;
    }

    @Override
    protected void onRecovery(CoordinatorServerMessages.Recovery msg) {

    }

    static public Props props(int numServers) { return Props.create(Checker.class, () -> new Checker(numServers)); }

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

    public void onCheckCorrectnessResponse (Message.CheckCorrectnessResponse msg) {
        counter++;
        partialSum += msg.value;
        if(counter == numServers){
            System.out.println("##### CORRECTNESS CHECK #####");
            System.out.println("CORRECT SUM: " + numServers * (Server.DB_SIZE * Server.DEFAULT_VALUE));
            System.out.println("ACTUAL SUM: " + partialSum);
            System.out.println("##### CORRECTNESS CHECK #####");
        }
    }

}