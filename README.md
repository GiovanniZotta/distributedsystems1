# Concurrent 2-phase commit

Implementation of a distributed system which handles transactions concurrently while guaranteeing strict serializability via a concurrent 2-Phase Commit protocol among multiple servers and coordinators. 

The system is made by several clients which can start transactions by contacting a coordinatorand sending to it a sequence of read/write operations on resources. Upon receiving a request, thecoordinator forwards them to the servers holding the requested resource and forwards the answerback to the client if needed. The servers follow an optimistic concurrency control: they perform theoperations on a *private* copy of the database, and only after the client has said that the transactionhas ended, the updates can possibly be propagated to the *oﬀicial* database. The latter phase is donethrough a validation process based on 2-PC, where servers agree on whether to commit or abortthe transaction, in such a way that concurrent transactions do not conflict and that the number ofcommitted transactions is maximized.

The file `DS1_report.pdf` contains a more detailed description of the system.

## Installation

The project is implemented in Java using the Akka framework.

1. Download and install Java SDK, Standard Edition. Version 11 is recommended for compatibility reasons.
2. Install [Gradle](https://gradle.org/install/) build tool
3. In the command line, change dir to  and run the project:
    ```[bash]
    cd concurrent_2pc
    gradle run
    ```