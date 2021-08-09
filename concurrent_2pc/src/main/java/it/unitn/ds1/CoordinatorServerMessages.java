package it.unitn.ds1;

import akka.actor.ActorRef;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CoordinatorServerMessages {
    // Start message that sends the list of participants to everyone
    public static class StartMessage implements Serializable {
        public final List<ActorRef> group;

        public StartMessage(List<ActorRef> group) {
            this.group = Collections.unmodifiableList(new ArrayList<>(group));
        }
    }

    public enum Vote {NO, YES}

    public enum Decision {ABORT, COMMIT}

    public static class VoteRequest implements Serializable {
    }

    public static class VoteResponse implements Serializable {
        public final Vote vote;

        public VoteResponse(Vote v) {
            vote = v;
        }
    }

    public static class DecisionRequest implements Serializable {
    }

    public static class DecisionResponse implements Serializable {
        public final Decision decision;

        public DecisionResponse(Decision d) {
            decision = d;
        }
    }

    public static class Timeout implements Serializable {
    }

    public static class Recovery implements Serializable {
    }

    /*-- Message classes ------------------------------------------------------ */

    public static class TransactionRead implements Serializable {
        public final Transaction transaction;
        public final Integer key;

        public TransactionRead(Transaction transaction, Integer key) {
            this.transaction = transaction;
            this.key = key;
        }
    }

    public static class TransactionWrite implements Serializable {
        public final Transaction transaction;
        public final Integer key;
        public final Integer value;

        public TransactionWrite(Transaction transaction, Integer key, Integer value) {
            this.transaction = transaction;
            this.key = key;
            this.value = value;
        }
    }
}
