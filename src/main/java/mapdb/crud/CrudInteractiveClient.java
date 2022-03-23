package mapdb.crud;

import org.apache.ratis.client.RaftClient;
import org.json.JSONObject;
import java.io.IOException;
import java.util.Scanner;

public class CrudInteractiveClient {

    public static void main(String[] args) throws IOException {

        RaftClient raftClient = CrudClient.buildClient();

        JSONObject clientRequest = new JSONObject();

        Scanner stdin = new Scanner(System.in);

        String key, value, result;

        while(true) {
            System.out.println("- - - - - - - - - - - - - - - -");
            System.out.println("Select an option:");
            System.out.println("1 - Put value");
            System.out.println("2 - Get value");
            System.out.println("3 - Delete Value");
            System.out.println("4 - Update Value");
            System.out.println("5 - Size of mapdb");
            System.out.println("6 - List keyset");
            System.out.println("7 - Exit");

            System.out.print("Option: ");
            int cmd = stdin.nextInt();
            clientRequest.clear();

            switch (cmd) {
                case 1 -> {
                    System.out.println("\nPut value");
                    System.out.print("Key: ");
                    key = stdin.next();
                    System.out.print("Value: ");
                    value = stdin.next();

                    clientRequest.put("REQUEST", "PUT");
                    clientRequest.put("KEY", key);
                    clientRequest.put("VALUE", value);

                    System.out.println("Response: " + CrudClient.sendAndGetClientReply(raftClient, clientRequest.toString()));
                }
                case 2 -> {
                    System.out.println("\nGet value");
                    System.out.print("Key: ");
                    key = stdin.next();

                    clientRequest.put("REQUEST", "GET");
                    clientRequest.put("KEY", key);

                    System.out.println("Response: " + CrudClient.sendReadOnlyAndGetClientReply(raftClient, clientRequest.toString()));
                }
                case 3 -> {
                    System.out.println("\nRemove value");
                    System.out.print("Key: ");
                    key = stdin.next();

                    clientRequest.put("REQUEST", "DELETE");
                    clientRequest.put("KEY", key);

                    System.out.println("\nResponse: " + CrudClient.sendAndGetClientReply(raftClient, clientRequest.toString()));
                }
                case 4 -> {
                    System.out.println("\nUpdate value");
                    System.out.print("Key: ");
                    key = stdin.next();
                    System.out.print("Value: ");
                    value = stdin.next();

                    clientRequest.put("REQUEST", "UPDATE");
                    clientRequest.put("KEY", key);
                    clientRequest.put("VALUE", value);

                    System.out.println("Response: " + CrudClient.sendAndGetClientReply(raftClient, clientRequest.toString()));
                }
                case 5 -> {
                    clientRequest.put("REQUEST", "SIZE");
                    System.out.println("\nSize: " + CrudClient.sendReadOnlyAndGetClientReply(raftClient, clientRequest.toString()));
                }
                case 6 -> {
                    clientRequest.put("REQUEST", "KEYSET");
                    System.out.println("\nKey-set: " + CrudClient.sendReadOnlyAndGetClientReply(raftClient, clientRequest.toString()));
                }
                case 7 -> {
                        System.exit(0);
                }
                default -> {
                }
            }

        }
    }

}
