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
import java.util.Scanner;

public class CrudInteractiveClient {

    public static void main(String[] args) throws IOException {

        RaftClient raftClient = buildClient();

        JSONObject clientRequest = new JSONObject();
        clientRequest.put("REQUEST", "");

        Scanner stdin = new Scanner(System.in);

        boolean exit = false;
        String key, value, result;

        while(!exit) {
            System.out.println("Select an option:");
            System.out.println("1 - Put value");
            System.out.println("2 - Get value");
            System.out.println("3 - Delete Value");
            System.out.println("4 - Update Value");
            System.out.println("5 - Size of mapdb");
            System.out.println("6 - List keyset");

            System.out.print("Option: ");
            int cmd = stdin.nextInt();
            clientRequest.clear();

            switch (cmd) {
                case 1 -> {
                    System.out.println("\nPutting value in the map");
                    System.out.print("Key: ");
                    key = stdin.next();
                    System.out.print("Value: ");
                    value = stdin.next();

                    clientRequest.put("REQUEST", "PUT");
                    clientRequest.put("KEY", key);
                    clientRequest.put("VALUE", value);

                    System.out.println("Response: " + sendAndGetClientReply(raftClient, clientRequest.toString()));
                }
                case 2 -> {
                    System.out.println("\nReading value from the map");
                    System.out.print("Key: ");
                    key = stdin.next();

                    clientRequest.put("REQUEST", "GET");
                    clientRequest.put("KEY", key);

                    System.out.println("Response: " + sendReadOnlyAndGetClientReply(raftClient, clientRequest.toString()));
                }
                case 3 -> {
                    System.out.println("\nRemoving value in the map");
                    System.out.print("Key: ");
                    key = stdin.next();

                    clientRequest.put("REQUEST", "DELETE");
                    clientRequest.put("KEY", key);

                    System.out.println("Response: " + sendAndGetClientReply(raftClient, clientRequest.toString()));
                }
                case 4 -> {
                    System.out.println("\nUpdatting value in the map");
                    System.out.print("Key: ");
                    key = stdin.next();
                    System.out.print("Value: ");
                    value = stdin.next();

                    clientRequest.put("REQUEST", "UPDATE");
                    clientRequest.put("KEY", key);
                    clientRequest.put("VALUE", value);

                    System.out.println("Response: " + sendAndGetClientReply(raftClient, clientRequest.toString()));
                }
                case 5 -> {
                    System.out.println("\nGetting the map size");
                    int size = 2;
                    System.out.println("Map size: " + size);}
                case 6 -> {
                    System.out.println("\nKey-set:");
                    clientRequest.put("REQUEST", "KEYSET");
                    System.out.println("Response: " + sendReadOnlyAndGetClientReply(raftClient, clientRequest.toString()));
                }
                default -> {
                }
            }
        }

    }

    private static String sendAndGetClientReply(RaftClient raftClient, String clientRequest) throws IOException {
        RaftClientReply reply =  raftClient.io().send(Message.valueOf(clientRequest));
        return reply.getMessage().getContent().toString(Charset.defaultCharset());
    }

    private static String sendReadOnlyAndGetClientReply(RaftClient raftClient, String clientRequest) throws IOException {
        RaftClientReply reply =  raftClient.io().sendReadOnly(Message.valueOf(clientRequest));
        return reply.getMessage().getContent().toString(Charset.defaultCharset());
    }

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
