package mapdb.crud;

import config.Config;
import mapdb.ycsb.YCSBMessage;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcFactory;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import java.io.IOException;
import java.nio.charset.Charset;

public class CrudClient {

    /**
     * build the RaftClient instance which is used to communicate to
     * Crud cluster
     *
     * @return the created client of Crud cluster
     */
    public static RaftClient buildClient() {
        RaftProperties raftProperties = new RaftProperties();
        RaftClient.Builder builder = RaftClient.newBuilder()
                .setProperties(raftProperties)
                .setRaftGroup(Config.RAFT_GROUP)
                .setClientRpc(
                        new GrpcFactory(new Parameters())
                                .newRaftClientRpc(ClientId.randomId(), raftProperties));
        return builder.build();
    }

    /**
     *  Send the request to the raft service with the objective
     *  of change the service state
     *
     * @return Message with the server response
     */

    public static String sendAndGetClientReply(RaftClient raftClient, ByteString clientRequest) throws IOException {
        RaftClientReply reply =  raftClient.io().send(Message.valueOf(clientRequest));
        return reply.getMessage().getContent().toString(Charset.defaultCharset());
    }

    /**
     *  Send the request to the raft service with the objective
     *  of readOnly
     *
     * @return Message with the server response
     */
    public static String sendReadOnlyAndGetClientReply(RaftClient raftClient, ByteString clientRequest) throws IOException {
        RaftClientReply reply =  raftClient.io().sendReadOnly(Message.valueOf(clientRequest));
        return reply.getMessage().getContent().toString(Charset.defaultCharset());
    }

    public static YCSBMessage sendAndGetClientReply2(RaftClient raftClient, ByteString clientRequest) throws IOException {
        RaftClientReply reply =  raftClient.io().send(Message.valueOf(clientRequest));
        return YCSBMessage.deserializeByteStringToObject(reply.getMessage().getContent());
    }


    public static YCSBMessage sendReadOnlyAndGetClientReply2(RaftClient raftClient, ByteString clientRequest) throws IOException {
        RaftClientReply reply =  raftClient.io().sendReadOnly(Message.valueOf(clientRequest));
        return YCSBMessage.deserializeByteStringToObject(reply.getMessage().getContent());
    }

}
