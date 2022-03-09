package counter.counter.server;

import counter.Config;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.util.NetUtils;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Scanner;

import static java.nio.charset.StandardCharsets.UTF_8;


public final class CounterServer implements Closeable {
  private final RaftServer server;

  public CounterServer(RaftPeer peer, File storageDir) throws IOException {
    //create a property object
    RaftProperties properties = new RaftProperties();

    //set the storage directory (different for each peer) in RaftProperty object
    RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(storageDir));

    //set the port which server listen to in RaftProperty object
    final int port = NetUtils.createSocketAddr(peer.getAddress()).getPort();
    GrpcConfigKeys.Server.setPort(properties, port);

    //create the counter state machine which hold the counter value
    CounterStateMachine counterStateMachine = new CounterStateMachine();

    //create and start the Raft server
    this.server = RaftServer.newBuilder()
        .setGroup(Config.RAFT_GROUP)
        .setProperties(properties)
        .setServerId(peer.getId())
        .setStateMachine(counterStateMachine)
        .build();

  }


  public void start() throws IOException {
    server.start();
  }

  @Override
  public void close() throws IOException {
    server.close();
  }

  public static void main(String[] args) throws IOException {
    if (args.length < 1) {
      System.err.println("Usage: java -cp *.jar org.apache.ratis.examples.counter.server.CounterServer {serverIndex}");
      System.err.println("{serverIndex} could be 1, 2 or 3");
      System.exit(1);
    }

    //find current peer object based on application parameter
    final RaftPeer currentPeer = Config.PEERS.get(Integer.parseInt(args[0]) - 1);

    //start a counter server
    final File storageDir = new File("./" + currentPeer.getId());
    final CounterServer counterServer = new CounterServer(currentPeer, storageDir);
    counterServer.start();

    //exit when any input entered
    Scanner scanner = new Scanner(System.in, UTF_8.name());
    scanner.nextLine();
    counterServer.close();
  }
}
