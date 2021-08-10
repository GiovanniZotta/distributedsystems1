package it.unitn.ds1.messages;

import akka.actor.ActorRef;
import it.unitn.ds1.messages.Message;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ClientCoordinatorMessages extends Message {

    // stop the client
    public static class StopMsg implements Serializable {}

    // message the client sends to a coordinator to begin the TXN
    public static class TxnBeginMsg implements Serializable {
        public final Integer clientId;
        public final Integer numAttemptedTxn;
        public TxnBeginMsg(int clientId, int numAttemptedTxn) {
            this.clientId = clientId;
            this.numAttemptedTxn = numAttemptedTxn;
        }
    }

    // reply from the coordinator receiving TxnBeginMsg
    public static class TxnAcceptMsg implements Serializable {}

    // the client may timeout waiting for TXN begin confirmation (TxnAcceptMsg)
    public static class TxnAcceptTimeoutMsg implements Serializable {}

    // message the client sends to a coordinator to end the TXN;
    // it may ask for commit (with probability COMMIT_PROBABILITY), or abort
    public static class TxnEndMsg implements Serializable {
        public final Integer clientId;
        public final Boolean commit; // if false, the transaction should abort
        public TxnEndMsg(int clientId, boolean commit) {
            this.clientId = clientId;
            this.commit = commit;
        }
    }

    // READ request from the client to the coordinator
    public static class ReadMsg implements Serializable {
        public final Integer clientId;
        public final Integer key; // the key of the value to read
        public ReadMsg(int clientId, int key) {
            this.clientId = clientId;
            this.key = key;
        }
    }

    // WRITE request from the client to the coordinator
    public static class WriteMsg implements Serializable {
        public final Integer clientId;
        public final Integer key; // the key of the value to write
        public final Integer value; // the new value to write
        public WriteMsg(int clientId, int key, int value) {
            this.clientId = clientId;
            this.key = key;
            this.value = value;
        }
    }

    // reply from the coordinator when requested a READ on a given key
    public static class ReadResultMsg implements Serializable {
        public final Integer key; // the key associated to the requested item
        public final Integer value; // the value found in the data store for that item
        public ReadResultMsg(int key, int value) {
            this.key = key;
            this.value = value;
        }
    }

    // message from the coordinator to the client with the outcome of the TXN
    public static class TxnResultMsg implements Serializable {
        public final Boolean commit; // if false, the transaction was aborted
        public TxnResultMsg(boolean commit) {
            this.commit = commit;
        }
    }

}
