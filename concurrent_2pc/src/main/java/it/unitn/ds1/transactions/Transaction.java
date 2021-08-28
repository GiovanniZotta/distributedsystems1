package it.unitn.ds1.transactions;


import java.util.AbstractMap;
import java.util.Map;

public abstract class Transaction implements Cloneable {

    private final Map.Entry<Integer, Integer> txnId;
    private State state;

    public Transaction(Integer clientId, Integer numAttemptedTxn) {
        this.txnId = new AbstractMap.SimpleEntry<>(clientId, numAttemptedTxn);
    }

    public Integer getClientId() {
        return txnId.getKey();
    }

    public Integer getNumAttemptedTxn() {
        return txnId.getValue();
    }

    public enum State {INIT, READY, DECIDED}

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null) return false;
        Transaction that = (Transaction) o;
        return txnId.equals(that.txnId);
    }

    @Override
    public int hashCode() {
        return txnId.hashCode();
    }


//    @Override
//    public Object clone() {
//        return new Transaction(txnId.getKey(), txnId.getValue());
//    }

    public Map.Entry<Integer, Integer> getTxnId() {
        return txnId;
    }

    public State getState() {
        return state;
    }

    // the state is final
    public static class UnmodifiableTransaction extends Transaction {
        private final State state;

        public UnmodifiableTransaction(Transaction t) {
            super(t.getClientId(), t.getNumAttemptedTxn());
            this.state = t.state;
        }
    }
    // the state can be modified
    public static  class ModifiableTransaction extends Transaction {
        protected State state;

        public ModifiableTransaction(Integer clientId, Integer numAttemptedTxn) {
            super(clientId, numAttemptedTxn);
            this.state = State.INIT;
        }

        public void setState(State state) {
            this.state = state;
        }
    }





}
