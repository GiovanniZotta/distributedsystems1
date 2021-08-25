package it.unitn.ds1.transactions;


import java.util.AbstractMap;
import java.util.Map;

public class Transaction implements Cloneable {
    public enum State {INIT, READY, DECIDED}

    private final Map.Entry<Integer, Integer> txnId;
    private State state;

    public Transaction(Integer clientId, Integer numAttemptedTxn) {
        this.txnId = new AbstractMap.SimpleEntry<>(clientId, numAttemptedTxn);
        state = State.INIT;
    }

    public Integer getClientId() {
        return txnId.getKey();
    }

    public Integer getNumAttemptedTxn() {
        return txnId.getValue();
    }

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


    @Override
    public Object clone() {
        return new Transaction(txnId.getKey(), txnId.getValue());
    }

    public Map.Entry<Integer, Integer> getTxnId() {
        return txnId;
    }

    public State getState() {
        return state;
    }

    public void setState(State state) {
        this.state = state;
    }

}
