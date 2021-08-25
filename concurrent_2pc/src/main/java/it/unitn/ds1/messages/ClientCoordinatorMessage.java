package it.unitn.ds1.messages;

public abstract class ClientCoordinatorMessage extends Message {
    public final Integer clientId;
    public final Integer numAttemptedTxn;

    public ClientCoordinatorMessage(Integer clientId, Integer numAttemptedTxn) {
        this.clientId = clientId;
        this.numAttemptedTxn = numAttemptedTxn;
    }

    // message the client sends to a coordinator to begin the TXN
    public static class TxnBeginMsg extends ClientCoordinatorMessage {
        public TxnBeginMsg(int clientId, int numAttemptedTxn) {
            super(clientId, numAttemptedTxn);
        }
    }

    // reply from the coordinator receiving TxnBeginMsg
    public static class TxnAcceptMsg extends ClientCoordinatorMessage {
        public TxnAcceptMsg(Integer clientId, Integer numAttemptedTxn) {
            super(clientId, numAttemptedTxn);
        }
    }

    // message the client sends to a coordinator to end the TXN;
    // it may ask for commit (with probability COMMIT_PROBABILITY), or abort
    public static class TxnEndMsg extends ClientCoordinatorMessage {
        public final Boolean commit; // if false, the transaction should abort

        public TxnEndMsg(Integer clientId, Integer numAttemptedTxn, Boolean commit) {
            super(clientId, numAttemptedTxn);
            this.commit = commit;
        }
    }

    // READ request from the client to the coordinator
    public static class ReadMsg extends ClientCoordinatorMessage {
        public final Integer key; // the key of the value to read

        public ReadMsg(Integer clientId, Integer numAttemptedTxn, Integer key) {
            super(clientId, numAttemptedTxn);
            this.key = key;
        }
    }

    // WRITE request from the client to the coordinator
    public static class WriteMsg extends ClientCoordinatorMessage {
        public final Integer key; // the key of the value to write
        public final Integer value; // the new value to write

        public WriteMsg(Integer clientId, Integer numAttemptedTxn, Integer key, Integer value) {
            super(clientId, numAttemptedTxn);
            this.key = key;
            this.value = value;
        }
    }

    // reply from the coordinator when requested a READ on a given key
    public static class ReadResultMsg extends ClientCoordinatorMessage {
        public final Integer key; // the key of the value to write
        public final Integer value; // the new value to write

        public ReadResultMsg(Integer clientId, Integer numAttemptedTxn, Integer key, Integer value) {
            super(clientId, numAttemptedTxn);
            this.key = key;
            this.value = value;
        }
    }

    // message from the coordinator to the client with the outcome of the TXN
    public static class TxnResultMsg extends ClientCoordinatorMessage {
        public final Boolean commit; // if false, the transaction was aborted

        public TxnResultMsg(Integer clientId, Integer numAttemptedTxn, Boolean commit) {
            super(clientId, numAttemptedTxn);
            this.commit = commit;
        }
    }

}
