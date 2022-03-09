package counter;

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.util.NetUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Scanner;

public class CounterServer {
    private CounterServer(){
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.err.println("Usage: java -cp *.jar org.apache.ratis.examples.counter.server.CounterServer {serverIndex}");
            System.err.println("{serverIndex} could be 1, 2 or 3");
            System.exit(1);
        }

        //find current peer object based on application parameter
        RaftPeer currentPeer =
                Config.PEERS.get(Integer.parseInt(args[0]) - 1);
        System.out.println("Node: " + currentPeer.getId().toString() + "\nAddress: " + currentPeer.getAddress());

        //set the storage directory (different for each peer) in RaftProperty object
        String path = "./" + currentPeer.getId().toString();
        File raftStorageDir = new File(path);
        System.out.println("Storage dir: " + path);

        //create a property object
        RaftProperties properties = new RaftProperties();

        RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(raftStorageDir));

        //set the port which server listen to in RaftProperty object
        final int port = NetUtils.createSocketAddr(currentPeer.getAddress()).getPort();
        GrpcConfigKeys.Server.setPort(properties, port);

        //create the counter state machine which hold the counter value
        CounterStateMachine counterStateMachine = new CounterStateMachine();

        //create and start the Raft server
        RaftServer server = RaftServer.newBuilder()
                .setGroup(Config.RAFT_GROUP)
                .setProperties(properties)
                .setServerId(currentPeer.getId())
                //.setStateMachine(counterStateMachine)
                .build();
        server.start();



        //exit when any input entered
        Scanner scanner = new Scanner(System.in, StandardCharsets.UTF_8.name());
        scanner.nextLine();
        server.close();
    }
}