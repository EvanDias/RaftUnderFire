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

        this.mapServer = new MapDBServer("src/main/java/mapdb/files/"+svSplitted[0]+".db", "map"+svSplitted[0]);
        //this.mapServer.map.put("chave","valor");

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

     //Handle GET command, which used by clients to get
    @Override
    public CompletableFuture<Message> query(Message request) {
        String msg = request.getContent().toString(Charset.defaultCharset());
        String[] msgSplitted = msg.split(",");

        if (!msgSplitted[0].equals("GET")) {
            return CompletableFuture.completedFuture(
                    Message.valueOf("Invalid Command"));
        }

        String value = this.mapServer.map.get(msgSplitted[1]);

        return CompletableFuture.completedFuture(
                Message.valueOf(value));
    }

    @Override
    public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
        final RaftProtos.LogEntryProto entry = trx.getLogEntry();

        //check if the command is valid
        String logData = entry.getStateMachineLogEntry().getLogData()
                .toString(Charset.defaultCharset());

        String[] logDataSplitted = logData.split(",");

        if (!logDataSplitted[0].equals("PUT")) {
            return CompletableFuture.completedFuture(
                    Message.valueOf("Invalid Command"));
        }
        //update the last applied term and index
        final long index = entry.getIndex();
        updateLastAppliedTermIndex(entry.getTerm(), index);

        // put key and value
        String value = this.mapServer.map.put(logDataSplitted[1],logDataSplitted[2]);

        //return the new value of the counter to the client
        final CompletableFuture<Message> f =
                CompletableFuture.completedFuture(Message.valueOf("Put value: " + logDataSplitted[1] + " with key: " + logDataSplitted[2]));

        //if leader, log the incremented value and it's log index
        if (trx.getServerRole() == RaftProtos.RaftPeerRole.LEADER) {
            LOG.info("{}: term", index);
        }

        return f;
    }







}
