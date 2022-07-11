package mapdb.ycsb;

import mapdb.MapDB;
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
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public class YCSBStateMachine extends BaseStateMachine {
    public MapDB mapServer;

    private final SimpleStateMachineStorage storage = new SimpleStateMachineStorage();

    @Override
    public void initialize(RaftServer server, RaftGroupId groupId,
                           RaftStorage raftStorage) throws IOException {
        super.initialize(server, groupId, raftStorage);
        this.storage.init(raftStorage);
        load(storage.getLatestSnapshot());

        String sv = server.toString();
        String[] svSplitted = sv.split(":");

        this.mapServer = new MapDB("src/main/java/mapdb/files/" + svSplitted[0] + ".db", "map" + svSplitted[0]);
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
     *  GET, KEYSET and SIZE. They are ReadOnly commands which will be handled by
     *  this method
     * @param request client request that arrive in a String/Json format
     * @return message reply to client
     */
    @Override
    public CompletableFuture<Message> query(Message request)  {

        YCSBMessage ycsbRequest = YCSBMessage.deserializeByteStringToObject(ByteString.copyFrom(request.getContent().toByteArray()));
        YCSBMessage ycsbReply;

        String requestedValue;

        if (Objects.requireNonNull(ycsbRequest).getType() == YCSBMessage.Type.READ) {

            if (!this.mapServer.map.containsKey(ycsbRequest.getKey())) {
                ycsbReply = YCSBMessage.newErrorMessage("The key is not contained in the KEYSET!");
                return CompletableFuture.completedFuture(Message.valueOf(ycsbReply.serializeObjectToByteString()));
            } else {
                requestedValue = this.mapServer.getValue(ycsbRequest.getKey()); // returns the value contained in the mapdb
            }

            ycsbReply = YCSBMessage.newReply(requestedValue, YCSBMessage.ReplyStatus.OK);

        } else {
            ycsbReply = YCSBMessage.newErrorMessage("Bad request type!");
            return CompletableFuture.completedFuture(Message.valueOf(ycsbReply.serializeObjectToByteString()));
        }

        final long index = getLastAppliedTermIndex().getIndex();
        final long term = getLastAppliedTermIndex().getTerm();

        LOG.info("| Operation: {} | Index: {} | Term: {} | Key: {} | Reply: {} | Status: {}", ycsbRequest.getType().toString(), index, term, ycsbRequest.getKey(), requestedValue, ycsbReply.getStatus().toString());
        // TODO: MSG TO STRING

        return CompletableFuture.completedFuture(
                    Message.valueOf(ycsbReply.serializeObjectToByteString()));
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

        YCSBMessage ycsbRequest = YCSBMessage.deserializeByteStringToObject(ByteString.copyFrom(entry.getStateMachineLogEntry().getLogData().toByteArray()));
        YCSBMessage ycsbReply = null;

        //check if the command is valid
        String logData = entry.getStateMachineLogEntry().getLogData()
                .toString(Charset.defaultCharset());

        //update the last applied term and index
        final long index = entry.getIndex();
        final long term = entry.getTerm();
        updateLastAppliedTermIndex(term, index);


        switch (Objects.requireNonNull(ycsbRequest).getType()) {
            case CREATE -> {
                this.mapServer.putValue(ycsbRequest.getKey(), ycsbRequest.getValue()); // returns the value contained in the mapdb
                ycsbReply = YCSBMessage.newReply("Operation CREATE successfully performed!", YCSBMessage.ReplyStatus.OK);
            }
            case UPDATE -> {
                this.mapServer.updateValue(ycsbRequest.getKey(), ycsbRequest.getValue()); // returns the value contained in the mapdb
                ycsbReply = YCSBMessage.newReply("Operation UPDATE successfully performed!", YCSBMessage.ReplyStatus.OK);
            }
            default -> {
                ycsbReply = YCSBMessage.newErrorMessage("Bad request type!");
                return CompletableFuture.completedFuture(Message.valueOf(ycsbReply.serializeObjectToByteString()));
            }
        }

        // return success to client
        final CompletableFuture<Message> f =
                CompletableFuture.completedFuture(Message.valueOf(ycsbReply.serializeObjectToByteString()));

        // if leader, log the incremented value, and it's log index
        if (trx.getServerRole() == RaftProtos.RaftPeerRole.LEADER) {
            LOG.info("| Operation: {} | Index: {} | Term: {} | Key: {} | Status: {}", ycsbRequest.getType().toString(), index, term, ycsbRequest.getKey(), ycsbReply.getStatus().toString());
        }

        return f;
    }


}
