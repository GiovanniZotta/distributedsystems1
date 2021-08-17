# Concurrent 2-PC
1. The client *A* initiates a transaction *T* involving *r* resources by sending a *TXN_BEGIN* message to a random coordinator *C* (which assigns an ID to *T*).
2. The coordinator *C* sends a confirmations to *A*. If *A* does not receive the confirmation after a timeout, it retries after some time.
3. *A* sends R/W operations of the transaction to *C*, which forwards them to the server holding the resources. When a server receives a R/W operation related to *T*, it creates a private workspace for *T* if it does not already exist and applies the changes (W) or returns the value (R) and stores the version of the resource when it is accessed first.
4. The client sends a *TXN_COMMIT* or *TXN_ABORT* message to *C* which starts a 2-PC session with the servers involved in the transaction.
5. *C* sends a vote requests to the servers, which decide to abort or commit. The coordinator sends to each server the list of resources in the transaction that are related to that server only. 
    * The server decides to vote **COMMIT** if all the current versions are equal to the ones stored in the private workspace of *T*. Each server keeps a record of all the resources for which it voted **COMMIT** during a validation (not terminated yet). If any of the resources asked by the coordinator is involved in this list, then it votes **ABORT**.
    * The server decides to abort if any resource version is outdated.
6. If *C* receives at least one **ABORT**, it sends an **ABORT** message, otherwise if everyone voted **COMMIT**, it sends a **COMMIT** message.
7. The servers decide to abort or commit according to the message received by the server.

## CRASH HANDLING

### COORDINATOR CRASH

#### The client sends a TXN_BEGIN to a crashed coordinator
After a timeout, the client asks to another coordinator.
(What if the coordinator was just slow? Discard the second TXN_BEGIN)

#### The coordinator crashes during a TXN
COORDINATOR: After the coordinator recovers, it aborts all the pending transactions. 
CLIENT: After a timeout, the client aborts by doing nothing.
SERVER: **abort after a timeout**

#### The coordinator crashes during a 2-PC (after receiving TXN_END)

* before the server has received a vote request:
  * SERVER: after a timeout, abort
  * COORDINATOR: once it recovers, decides abort
* after the server has sent a YES vote, while waiting for the global decision:
  * COORDINATOR:
    * if it is in READY state, abort after a timeout if it does not receive all the votes (even if it has crashed and recovered)
    * if it is in COMMIT/ABORT on recovery tell the decision to the remaining cohorts.
  * SERVER: ask everybody else for the decision:
    * if it finds a COMMIT, commits;
    * if it finds an ABORT, aborts;
    * otherwise wait. (periodically ask?)
* the client times out and starts a new transaction with another (or the same) coordinator

### SERVER CRASH

#### A server crashes during a TXN
* COORDINATOR: on timeout, decide to abort
* SERVER: on recovery abort all pending transactions apart from the ones for which it voted COMMIT.

#### A server crashes during a 2-PC
* before the server sent a vote:
  * COORDINATOR: abort after a timeout
  * SERVER: on recovery abort all pending transactions apart from the ones for which it voted COMMIT.
* after the server sent a COMMIT vote
  * COORDINATOR: business as usual
  * SERVER: on recovery ask the decision to the coordinator 

## Considerations

### Private workspace and versions
* When a client reads a value, if it is the first time for that value it reads the most recent committed value and the version is stored in the private workspace along with the item version. Otherwise, it is read from the private workspace.
* The version is changed only on commit. The private workspaces are stored like:
  ```
  {
    TXN_ID: 
    {
      ITEM_KEY: (VERSION, VALUE, CHANGED)
    }
  }
  ```
  where
  * VERSION is the first version read/written
  * VALUE is the last value read/written
  * CHANGED is a boolean

### Reliable network
We assume that our timeout are much larger than the network propagation speed.