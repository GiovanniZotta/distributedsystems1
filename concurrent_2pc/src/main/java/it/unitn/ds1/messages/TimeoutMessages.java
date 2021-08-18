package it.unitn.ds1.messages;

import java.io.Serializable;

public abstract class TimeoutMessages extends Message {
    public static class Client {
        // the client may timeout waiting for TXN begin confirmation (TxnAcceptMsg)
        public static class TxnAcceptMsg implements Serializable {}


        public static class TxnOperationMsg implements Serializable {}
    }
}
