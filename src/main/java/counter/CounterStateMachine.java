package counter;


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
import org.apache.ratis.util.JavaUtils;

import java.io.*;
import java.nio.charset.Charset;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

public class CounterStateMachine extends BaseStateMachine {

    private final SimpleStateMachineStorage storage = new SimpleStateMachineStorage();
    private AtomicInteger counter = new AtomicInteger(0);

    /**
     *
     * Initializes the state machine by initializing the storage.
     * Also updates the counter variable if there was a previous one
     *
     * @param raftServer current server info
     * @param raftGroupId cluster groupId
     * @param storage storage used to keep raft info
     * @throws IOException in case any error occurs
     */

    @Override
    public void initialize(RaftServer raftServer, RaftGroupId raftGroupId,
                           RaftStorage storage) throws IOException {

        super.initialize(raftServer, raftGroupId, storage);
        this.storage.init((storage));
        load(this.storage.getLatestSnapshot());
    }

    @Override
    public void reinitialize() throws IOException {
        load(storage.getLatestSnapshot());
    }

    @Override
    public CompletableFuture<Message> query(Message request) {
        String msg = request.getContent().toString((Charset.defaultCharset()));
        if (!msg.equals("GET")) {
            return CompletableFuture.completedFuture(Message.valueOf("Invalid Command"));
        }
        return CompletableFuture.completedFuture(Message.valueOf(counter.toString()));
    }

    @Override
    public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
        final RaftProtos.LogEntryProto entryProto = trx.getLogEntry();

        String logData = entryProto.getStateMachineLogEntry().getLogData()
                .toString(Charset.defaultCharset());

        if (!logData.equals("INCREMENT")) {
            return CompletableFuture.completedFuture(Message.valueOf("Invalid Command"));
        }

        final long index = entryProto.getIndex();
        updateLastAppliedTermIndex(entryProto.getTerm(), index);

        counter.incrementAndGet();

        final CompletableFuture<Message> completableFuture = CompletableFuture.
                completedFuture(Message.valueOf(counter.toString()));

        if (trx.getServerRole() == RaftProtos.RaftPeerRole.LEADER) {
            LOG.info("{}: Increment to {}", index, counter.toString());
        }

        return completableFuture;
    }

    private long load(SingleFileSnapshotInfo snapshot) throws IOException{

        if (snapshot == null) {
            LOG.warn("The snapshop is null");
            return RaftLog.INVALID_LOG_INDEX;
        }

        final File snapshotFile = snapshot.getFile().getPath().toFile();
        if (!snapshotFile.exists()) {
            LOG.warn("The snapshot file {} does not exist for snapshot {}",
                    snapshotFile, snapshot);
            return RaftLog.INVALID_LOG_INDEX;
        }

        final TermIndex last = SimpleStateMachineStorage.getTermIndexFromSnapshotFile(snapshotFile);

        try (ObjectInputStream in = new ObjectInputStream(
                new BufferedInputStream(new FileInputStream(snapshotFile)))) {

            setLastAppliedTermIndex(last);
            counter = JavaUtils.cast(in.readObject());

        } catch (ClassNotFoundException exception) {
            throw new IllegalStateException(exception);
        }
        return last.getIndex();
    }

    @Override
    public long takeSnapshot() throws IOException {

        final TermIndex last = getLastAppliedTermIndex();

        final File snapshotFile = storage.getSnapshotFile(last.getTerm(), last.getIndex());

        try (ObjectOutputStream out = new ObjectOutputStream(new BufferedOutputStream(
                new FileOutputStream(snapshotFile)))) {

            out.writeObject(counter);

        } catch (IOException exception) {
            LOG.warn("Failed to write snapshot file \"" + snapshotFile
                    + "\", last applied index=" + last);
        }

        return last.getIndex();

    }
}