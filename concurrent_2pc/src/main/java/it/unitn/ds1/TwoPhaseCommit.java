package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.ds1.CoordinatorServerMessages.StartMessage;
import it.unitn.ds1.CoordinatorServerMessages.Vote;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TwoPhaseCommit {
    final static int N_PARTICIPANTS = 3;
    final static int VOTE_TIMEOUT = 1000;      // timeout for the votes, ms
    final static int DECISION_TIMEOUT = 2000;  // timeout for the decision, ms

    // the votes that the participants will send (for testing)
    final static Vote[] predefinedVotes =
            new Vote[]{Vote.YES, Vote.YES, Vote.YES}; // as many as N_PARTICIPANTS



    /*-- Main ------------------------------------------------------------------*/
    public static void main(String[] args) {

        // Create the actor system
        final ActorSystem system = ActorSystem.create("helloakka");

        // Create the coordinator
        ActorRef coordinator = system.actorOf(Coordinator.props(), "coordinator");

        // Create participants
        List<ActorRef> group = new ArrayList<>();
        for (int i = 0; i < N_PARTICIPANTS; i++) {
            group.add(system.actorOf(Server.props(i), "participant" + i));
        }

        // Send start messages to the participants to inform them of the group
        StartMessage start = new StartMessage(group);
        for (ActorRef peer : group) {
            peer.tell(start, null);
        }

        // Send the start messages to the coordinator
        coordinator.tell(start, null);

        try {
            System.out.println(">>> Press ENTER to exit <<<");
            System.in.read();
        } catch (IOException ignored) {
        }
        system.terminate();
    }
}
