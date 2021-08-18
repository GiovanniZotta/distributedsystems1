package it.unitn.ds1.messages;

import java.io.Serializable;

public abstract class ClientCoordinatorMessage extends Message {


    // message the client sends to a coordinator to begin the TXN
    public static class TxnBeginMsg extends ClientCoordinatorMessage {
        public final Integer clientId;
        public final Integer numAttemptedTxn;

        public TxnBeginMsg(int clientId, int numAttemptedTxn) {
            this.clientId = clientId;
            this.numAttemptedTxn = numAttemptedTxn;
        }
    }

    // reply from the coordinator receiving TxnBeginMsg
    public static class TxnAcceptMsg extends ClientCoordinatorMessage {}

    // message the client sends to a coordinator to end the TXN;
    // it may ask for commit (with probability COMMIT_PROBABILITY), or abort
    public static class TxnEndMsg extends ClientCoordinatorMessage {
        public final Integer clientId;
        public final Boolean commit; // if false, the transaction should abort
        public TxnEndMsg(int clientId, boolean commit) {
            this.clientId = clientId;
            this.commit = commit;
        }
    }

    // READ request from the client to the coordinator
    public static class ReadMsg extends ClientCoordinatorMessage {
        public final Integer clientId;
        public final Integer key; // the key of the value to read
        public ReadMsg(int clientId, int key) {
            this.clientId = clientId;
            this.key = key;
        }
    }

    // WRITE request from the client to the coordinator
    public static class WriteMsg extends ClientCoordinatorMessage {
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
    public static class ReadResultMsg extends ClientCoordinatorMessage {
        public final Integer key; // the key associated to the requested item
        public final Integer value; // the value found in the data store for that item
        public ReadResultMsg(int key, int value) {
            this.key = key;
            this.value = value;
        }
    }

    // message from the coordinator to the client with the outcome of the TXN
    public static class TxnResultMsg extends ClientCoordinatorMessage {
        public final Boolean commit; // if false, the transaction was aborted
        public final Integer numAttemptedTxn;
        public TxnResultMsg(boolean commit, Integer numAttemptedTxn) {
            this.commit = commit;
            this.numAttemptedTxn = numAttemptedTxn;
        }
    }

}
