package mapdb.crud;

import mapdb.MapDBServer;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.json.JSONObject;

import java.io.*;
import java.nio.charset.Charset;
import java.util.concurrent.CompletableFuture;

public class CrudStateMachine extends BaseStateMachine {

    private final SimpleStateMachineStorage storage =
            new SimpleStateMachineStorage();

    public MapDBServer mapServer;

    // ------------------------------------------------------------------------------------------------------------------ //

    @Override
    public void initialize(RaftServer server, RaftGroupId groupId,
                           RaftStorage raftStorage) throws IOException {
        super.initialize(server, groupId, raftStorage);
        this.storage.init(raftStorage);
        load(storage.getLatestSnapshot());

        String sv = server.toString();
        String[] svSplitted = sv.split(":");

        this.mapServer = new MapDBServer("src/main/java/mapdb/files/" + svSplitted[0] + ".db", "map" + svSplitted[0]);

    }

    // ------------------------------------------------------------------------------------------------------------------ //

    @Override
    public void reinitialize() throws IOException {
        load(storage.getLatestSnapshot());
    }

    // ------------------------------------------------------------------------------------------------------------------ //

    // Load the state of the state machine from the storage.
    private long load(SingleFileSnapshotInfo snapshot) throws IOException {

        //check the snapshot nullity
        if (snapshot == null) {
            LOG.warn("The snapshot info is null.");
            return RaftLog.INVALID_LOG_INDEX;
        }

        //check the existence of the snapshot file
        final File snapshotFile = snapshot.getFile().getPath().toFile();
        if (!snapshotFile.exists()) {
            LOG.warn("The snapshot file {} does not exist for snapshot {}",
                    snapshotFile, snapshot);
            return RaftLog.INVALID_LOG_INDEX;
        }

        //load the TermIndex object for the snapshot using the file name pattern of
        // the snapshot
        final TermIndex last =
                SimpleStateMachineStorage.getTermIndexFromSnapshotFile(snapshotFile);

        return last.getIndex();
    }

    // ------------------------------------------------------------------------------------------------------------------ //

     //Handle GET commands, which used by clients to get a value through a key
    @Override
    public CompletableFuture<Message> query(Message request) {

        String msg = request.getContent().toString(Charset.defaultCharset()); // msg arrive in json string format by the client

        JSONObject clientRequest = new JSONObject(msg);

        if (!clientRequest.has("REQUEST")) {
            return CompletableFuture.completedFuture(
                    Message.valueOf("There is no REQUEST argument in the message body")); }

        String operation, requestedValue;

        switch (clientRequest.get("REQUEST").toString()) {
            case "GET":
                requestedValue = this.mapServer.map.get(clientRequest.get("KEY").toString()); // returns the value contained in the mapdb
                operation = "PUT";
                break;

            case "KEYSET":
                requestedValue = this.mapServer.map.keySet().toString();
                operation = "KEYSET";
                break;

            case "SIZE":
                requestedValue = String.valueOf(this.mapServer.map.size());
                operation = "SIZE";
                break;

            default:
                return CompletableFuture.completedFuture(
                        Message.valueOf("Invalid request type!"));
        }

        LOG.info("{} operation performed", operation);

        if(requestedValue == null)
            return CompletableFuture.completedFuture(Message.EMPTY);
        else
            return CompletableFuture.completedFuture(
                    Message.valueOf(requestedValue));
    }

    @Override
    public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
        final RaftProtos.LogEntryProto entry = trx.getLogEntry();

        //check if the command is valid
        String logData = entry.getStateMachineLogEntry().getLogData()
                .toString(Charset.defaultCharset());

        //trx.getLogEntry().getMetadataEntry().toString();

        JSONObject clientRequest = new JSONObject(logData);

        if (!clientRequest.has("REQUEST")) {
            return CompletableFuture.completedFuture(
                    Message.valueOf("There is no REQUEST argument in the message body")); }

        //update the last applied term and index
        final long index = entry.getIndex();
        updateLastAppliedTermIndex(entry.getTerm(), index);

        String operation;

        switch (clientRequest.get("REQUEST").toString()) {
            case "PUT":
                this.mapServer.map.put(clientRequest.get("KEY").toString(), clientRequest.get("VALUE").toString());
                operation = "PUT";
                break;

            case "UPDATE":
                this.mapServer.map.replace(clientRequest.get("KEY").toString(), clientRequest.get("VALUE").toString());
                operation = "UPDATE";
                break;

            case "DELETE":
                this.mapServer.map.remove(clientRequest.get("KEY").toString());
                operation = "DELETE";
                break;

            default:
                return CompletableFuture.completedFuture(
                        Message.valueOf("Invalid request type!"));

        }

        // return success to client
        final CompletableFuture<Message> f =
                CompletableFuture.completedFuture(Message.valueOf("200 SUCCESS!"));

        // if leader, log the incremented value and it's log index
        if (trx.getServerRole() == RaftProtos.RaftPeerRole.LEADER) {
            LOG.info("{} operation performed - {}: term", operation, index);
        }

        return f;
    }







}
