package it.unitn.ds1;

import scala.Tuple2;

public class Transaction {
    private Tuple2<Integer, Integer> txnId;

    public Transaction(Integer clientId, Integer numAttemptedTxn) {
        this.txnId = new Tuple2<>(clientId, numAttemptedTxn);
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
}
