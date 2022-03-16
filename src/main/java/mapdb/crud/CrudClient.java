package mapdb.crud;

import counter.Config;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcFactory;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class CrudClient {

    private CrudClient(){
    }

    public static void main(String[] args)
            throws IOException, InterruptedException {

        RaftClient raftClient = buildClient();

        //RaftClientReply putValue = raftClient.io().send(Message.valueOf("PUT,mykey10,myvalue10"));

        //send GET command and print the response
        RaftClientReply getValue = raftClient.io().sendReadOnly(Message.valueOf("GET,mykey10"));
        String response = getValue.getMessage().getContent().toString(Charset.defaultCharset());
        System.out.println("Value: " + response);

    }

    // ------------------------------------------------------------------------------------------------------------------

    /**
     * build the RaftClient instance which is used to communicate to
     * Crud cluster
     *
     * @return the created client of Crud cluster
     */
    private static RaftClient buildClient() {
        RaftProperties raftProperties = new RaftProperties();
        RaftClient.Builder builder = RaftClient.newBuilder()
                .setProperties(raftProperties)
                .setRaftGroup(Config.RAFT_GROUP)
                .setClientRpc(
                        new GrpcFactory(new Parameters())
                                .newRaftClientRpc(ClientId.randomId(), raftProperties));
        return builder.build();
    }
}
