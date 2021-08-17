package it.unitn.ds1.messages;

import java.io.Serializable;

public class TimeoutMessages {
    public static class Client {
        // the client may timeout waiting for TXN begin confirmation (TxnAcceptMsg)
        public static class TxnAcceptMsg implements Serializable {}


        public static class TxnOperationMsg implements Serializable {}
    }
}
