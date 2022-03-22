package mapdb.crud;

import counter.Config;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcFactory;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.json.JSONObject;

import java.io.IOException;
import java.nio.charset.Charset;

public class CrudClient {

    private CrudClient(){
    }


    public static void main(String[] args)
            throws IOException, InterruptedException {


        RaftClient raftClient = buildClient();

        JSONObject requestJson = new JSONObject();
        requestJson.put("REQUEST", "PUT");
        requestJson.put("KEY", "mykey9");
        requestJson.put("VALUE", "myvalue9");


        //String requestMsg = requestJson.toString();
        //RaftClientReply putValue = raftClient.io().send(Message.valueOf(requestMsg));
        //String response = putValue.getMessage().getContent().toString(Charset.defaultCharset());
        //System.out.println("Request response: " + response);


        JSONObject requestJson2 = new JSONObject();
        requestJson2.put("REQUEST", "KEYSET");
        //requestJson2.put("KEY", "mykey6");
        String requestMsg2 = requestJson2.toString();


        //send GET command and print the response
        RaftClientReply getValue = raftClient.io().sendReadOnly(Message.valueOf(requestMsg2));
        String response2 = getValue.getMessage().getContent().toString(Charset.defaultCharset());
        System.out.println("Value: " + response2);



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
