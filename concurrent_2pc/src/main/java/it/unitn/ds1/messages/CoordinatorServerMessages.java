package it.unitn.ds1.messages;

import akka.actor.ActorRef;
import it.unitn.ds1.Transaction;
import it.unitn.ds1.messages.Message;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CoordinatorServerMessages extends Message {

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
    public static abstract class TransactionAction implements Serializable {
        public final Transaction transaction;
        public final Integer key;

        public TransactionAction(Transaction transaction, Integer key) {
            this.transaction = (Transaction) transaction.clone();
            this.key = key;
        }
    }
    public static class TransactionRead extends TransactionAction implements Serializable {
        public TransactionRead(Transaction transaction, Integer key) {
            super(transaction, key);
        }
    }


    public static class TransactionWrite extends TransactionAction implements Serializable {
        public final Integer value;

        public TransactionWrite(Transaction transaction, Integer key, Integer value) {
            super(transaction, key);
            this.value = value;
        }
    }

    public static class TransactionReadResponse extends TransactionAction implements Serializable {
        public final Integer valueRead;

        public TransactionReadResponse(Transaction transaction, Integer key, Integer valueRead) {
            super(transaction, key);
            this.valueRead = valueRead;
        }
    }
}
