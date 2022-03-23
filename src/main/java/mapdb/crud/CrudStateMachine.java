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


    @Override
    public void initialize(RaftServer server, RaftGroupId groupId,
                           RaftStorage raftStorage) throws IOException {
        super.initialize(server, groupId, raftStorage);
        this.storage.init(raftStorage);
        load(storage.getLatestSnapshot());

        String sv = server.toString();
        String[] svSplitted = sv.split(":");

        this.mapServer = new MapDBServer("src/main/java/mapdb/files/" + svSplitted[0] + ".db", "map" + svSplitted[0]);

        //LogEntryHeader[] entries = server.getDivision(groupId).getRaftLog().getEntries(0,5);
        //for (LogEntryHeader entry : entries) {
        //    System.out.println("ENTRY: " + entry.toString());
       //}
    }


    @Override
    public void reinitialize() throws IOException {
        load(storage.getLatestSnapshot());
    }


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


    /**
     *  This class maintain a HTreeMap (MapDB) and accept many commands/requests:
     *  GET, KEYSET and SIZE, GET. They are ReadOnly commands which will be handled by
     *  this method
     * @param request client request that arrive in a String/Json format
     * @return message reply to client
     */
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
                requestedValue = this.mapServer.getValue(clientRequest.get("KEY").toString()); // returns the value contained in the mapdb
                operation = "GET";
                break;

            case "KEYSET":
                requestedValue = this.mapServer.getKeySet();
                operation = "KEYSET";
                break;

            case "SIZE":
                requestedValue = this.mapServer.getSize();
                operation = "SIZE";
                break;

            default:
                return CompletableFuture.completedFuture(
                        Message.valueOf("Invalid request type!"));
        }

        final long index = getLastAppliedTermIndex().getIndex();
        final long term = getLastAppliedTermIndex().getTerm();

        LOG.info("| Operation: {} | Index: {} | Term: {} | LogEntry: {} | Reply: {}", operation, index, term, clientRequest.toString(), requestedValue);

        if(requestedValue == null)
            return CompletableFuture.completedFuture(Message.EMPTY);
        else
            return CompletableFuture.completedFuture(
                    Message.valueOf(requestedValue));
    }

    /**
     *  This class maintain a HTreeMap (MapDB) and accept many commands/requests:
     *  PUT, UPDATE AND DELETE. They are transactional commands which will be handled by
     *  this method
     * @param trx committed entry coming from the RAFT log from the leader
     * @return message reply to client
     */
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
        final long term = entry.getTerm();
        updateLastAppliedTermIndex(term, index);

        String operation;

        switch (clientRequest.get("REQUEST").toString()) {
            case "PUT":
                this.mapServer.putValue(clientRequest.get("KEY").toString(), clientRequest.get("VALUE").toString());
                operation = "PUT";
                break;

            case "UPDATE":
                this.mapServer.updateValue(clientRequest.get("KEY").toString(), clientRequest.get("VALUE").toString());
                operation = "UPDATE";
                break;

            case "DELETE":
                this.mapServer.deleteValue(clientRequest.get("KEY").toString());
                operation = "DELETE";
                break;

            default:
                return CompletableFuture.completedFuture(
                        Message.valueOf("Invalid request type!"));

        }

        // return success to client
        final CompletableFuture<Message> f =
                CompletableFuture.completedFuture(Message.valueOf("Operation " + operation + " successfully performed!"));

        // if leader, log the incremented value and it's log index
        if (trx.getServerRole() == RaftProtos.RaftPeerRole.LEADER) {
            LOG.info("| Operation: {} | Index: {} | Term: {} | LogEntry: {} |", operation, index, term, clientRequest.toString());
        }

        return f;
    }


}
