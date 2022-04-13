package mapdb;

import config.Config;
import mapdb.crud.CrudStateMachine;
import mapdb.ycsb.YCSBStateMachine;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.util.NetUtils;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Scanner;

import static java.nio.charset.StandardCharsets.UTF_8;
public class RaftServer implements Closeable {
    private final org.apache.ratis.server.RaftServer server;
    public RaftServer(RaftPeer peer, File storageDir, BaseStateMachine stateMachine) throws IOException {
        //create a property object
        RaftProperties properties = new RaftProperties();

        //set the storage directory (different for each peer) in RaftProperty object
        RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(storageDir));

        //set the port which server listen to in RaftProperty object
        final int port = NetUtils.createSocketAddr(peer.getAddress()).getPort();
        GrpcConfigKeys.Server.setPort(properties, port);

        //create and start the Raft server
        this.server = org.apache.ratis.server.RaftServer.newBuilder()
                .setGroup(Config.RAFT_GROUP)
                .setProperties(properties)
                .setServerId(peer.getId())
                .setStateMachine(stateMachine)
                .build();
    }
    public void start() throws IOException {
        server.start();
    }

    @Override
    public void close() throws IOException {
        server.close();
    }
    public static void newCrudStateMachine(final RaftPeer currentPeer) throws IOException {

        CrudStateMachine stateMachine = new CrudStateMachine();

        //start a crud server
        final File storageDir = new File("./src/main/java/mapdb/crud/nodes/" + currentPeer.getId());
        final RaftServer raftServer = new RaftServer(currentPeer, storageDir, stateMachine);

        raftServer.start();

        //exit when any input entered
        Scanner scanner = new Scanner(System.in, UTF_8.name());
        scanner.nextLine();
        raftServer.close();

        stateMachine.mapServer.closedb();
        stateMachine.mapServer.closemap();
    }
    public static void newYCSBStateMachine(final RaftPeer currentPeer) throws IOException {

        YCSBStateMachine stateMachine = new YCSBStateMachine();

        //start a crud server
        final File storageDir = new File("./src/main/java/mapdb/ycsb/nodes/" + currentPeer.getId());
        final RaftServer raftServer = new RaftServer(currentPeer, storageDir, stateMachine);

        raftServer.start();

        //exit when any input entered
        Scanner scanner = new Scanner(System.in, UTF_8.name());
        scanner.nextLine();
        raftServer.close();

        stateMachine.mapServer.closedb();
        stateMachine.mapServer.closemap();
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.err.println("Usage: java -cp target/RaftUnderFire-1.0-SNAPSHOT.jar mapdb.RaftServer {serverIndex} {serverStateMachine}");
            System.err.println("{serverIndex} could be 1, 2 or 3");
            System.err.println("{serverStateMachine} could be crud, ycsb");
            System.exit(1);
        }

        if(args[1].equals("crud"))
            newCrudStateMachine(Config.PEERS.get(Integer.parseInt(args[0]) - 1));
        else if(args[1].equals("ycsb"))
            newYCSBStateMachine(Config.PEERS.get(Integer.parseInt(args[0]) - 1));
        else {
            System.err.println("{serverIndex} could be 1, 2 or 3");
            System.err.println("{serverStateMachine} could be crud, ycsb");
            System.exit(1);
        }

    }

}
