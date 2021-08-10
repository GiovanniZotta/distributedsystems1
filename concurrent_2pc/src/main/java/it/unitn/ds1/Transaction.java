package it.unitn.ds1;


import java.io.Serializable;
import java.util.AbstractMap;
import java.util.Map;

public class Transaction implements Cloneable {
    private Map.Entry<Integer, Integer> txnId;

    public Transaction(Integer clientId, Integer numAttemptedTxn) {
        this.txnId = new AbstractMap.SimpleEntry<>(clientId, numAttemptedTxn);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
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
}
