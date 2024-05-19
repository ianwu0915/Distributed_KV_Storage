
# raft-java
Raft implementation library for Java.<br>
Reference from [Raft Paper](https://github.com/maemual/raft-zh_cn) and Raft author's open source implementation [LogCabin](https://github.com/logcabin/logcabin).

# Supported Features
* Leader election
* Log replication 
* Snapshot
* Dynamic cluster membership changes

## Quick Start
Deploy a 3-instance raft cluster on local machine by executing the following script:<br>
cd raft-java-example && sh deploy.sh <br>
This script will deploy three instances (example1, example2, example3) in raft-java-example/env directory;<br>
It will also create a client directory for testing read/write functionality of the raft cluster.<br>
After successful deployment, test write operation with the following script:
cd env/client <br>
./bin/run_client.sh "list://127.0.0.1:8051,127.0.0.1:8052,127.0.0.1:8053" hello world <br>
Test read operation command:<br>
./bin/run_client.sh "list://127.0.0.1:8051,127.0.0.1:8052,127.0.0.1:8053" hello

# Usage
Below describes how to use the raft-java dependency library in code to implement a distributed storage system.
## Configure Dependencies (Not yet published to maven central repository, need to manually install locally)
```xml
<dependency>
    <groupId>com.github.raftimpl.raft</groupId>
    <artifactId>raft-java-core</artifactId>
    <version>1.9.0</version>
</dependency>
```

## Define Data Write and Read Interface
```protobuf
message SetRequest {
    string key = 1;
    string value = 2;
}
message SetResponse {
    bool success = 1;
}
message GetRequest {
    string key = 1;
}
message GetResponse {
    string value = 1;
}
```
```java
public interface ExampleService {
    Example.SetResponse set(Example.SetRequest request);
    Example.GetResponse get(Example.GetRequest request);
}
```

## Server-side Usage
1. Implement StateMachine Interface Implementation Class
```java
// These three interface methods are mainly called by Raft internally
public interface StateMachine {
    /**
     * Take snapshot of data in state machine, called periodically by each local node
     * @param snapshotDir snapshot data output directory
     */
    void writeSnapshot(String snapshotDir);
    /**
     * Read snapshot into state machine, called when node starts
     * @param snapshotDir snapshot data directory
     */
    void readSnapshot(String snapshotDir);
    /**
     * Apply data to state machine
     * @param dataBytes data binary
     */
    void apply(byte[] dataBytes);
}
```

2. Implement Data Write and Read Interface
```java
// ExampleService implementation class needs to include following members
private RaftNode raftNode;
private ExampleStateMachine stateMachine;
```
```java
// Main logic for data write
byte[] data = request.toByteArray();
// Synchronously write data to raft cluster
boolean success = raftNode.replicate(data, Raft.EntryType.ENTRY_TYPE_DATA);
Example.SetResponse response = Example.SetResponse.newBuilder().setSuccess(success).build();
```
```java
// Main logic for data read, implemented by specific application state machine
Example.GetResponse response = stateMachine.get(request);
```

3. Server Start-up Logic
```java
// Initialize RPCServer
RPCServer server = new RPCServer(localServer.getEndPoint().getPort());
// Application state machine
ExampleStateMachine stateMachine = new ExampleStateMachine();
// Set Raft options, for example:
RaftOptions.snapshotMinLogSize = 10 * 1024;
RaftOptions.snapshotPeriodSeconds = 30;
RaftOptions.maxSegmentFileSize = 1024 * 1024;
// Initialize RaftNode
RaftNode raftNode = new RaftNode(serverList, localServer, stateMachine);
// Register service for inter-node communication between Raft nodes
RaftConsensusService raftConsensusService = new RaftConsensusServiceImpl(raftNode);
server.registerService(raftConsensusService);
// Register Raft service for Client calls
RaftClientService raftClientService = new RaftClientServiceImpl(raftNode);
server.registerService(raftClientService);
// Register application's own service
ExampleService exampleService = new ExampleServiceImpl(raftNode, stateMachine);
server.registerService(exampleService);
// Start RPCServer, initialize Raft node
server.start();
raftNode.init();
```