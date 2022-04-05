package mapdb.crud;

import org.apache.ratis.client.RaftClient;
import org.json.JSONObject;
import java.io.IOException;
import java.util.Scanner;

public class CrudInteractiveClient {

    public static void main(String[] args) throws IOException {

        RaftClient raftClient = CrudClient.buildClient();

        Scanner stdin = new Scanner(System.in);

        String key, value, result;

        while(true) {
            System.out.println("- - - - - - - - - - - - - - - -");
            System.out.println("Select an option:");
            System.out.println("1 - Create value");
            System.out.println("2 - Read value");
            System.out.println("3 - Delete Value");
            System.out.println("4 - Update Value");
            System.out.println("5 - Size of mapdb");
            System.out.println("6 - List keyset");
            System.out.println("7 - Exit");
            System.out.println("8 - Test case with loops");

            System.out.print("Option: ");
            int cmd = stdin.nextInt();

            switch (cmd) {
                case 1 -> {
                    System.out.println("\nCreate value");
                    System.out.print("Key: ");
                    key = stdin.next();
                    System.out.print("Value: ");
                    value = stdin.next();

                    CrudMessage requestMsg = CrudMessage.newCreateRequest(key, value);

                    System.out.println("Response: " + CrudClient.sendAndGetClientReply(raftClient, requestMsg.serializeObjectToByteString()));
                }
                case 2 -> {

                    System.out.println("\nRead value");
                    System.out.print("Key: ");
                    key = stdin.next();

                    CrudMessage requestMsg = CrudMessage.newReadRequest(key);

                    System.out.println("Response: " + CrudClient.sendReadOnlyAndGetClientReply(raftClient, requestMsg.serializeObjectToByteString()));
                }

                case 3 -> {
                    System.out.println("\nRemove value");
                    System.out.print("Key: ");
                    key = stdin.next();

                    CrudMessage requestMsg = CrudMessage.newDeleteRequest(key);

                    System.out.println("\nResponse: " + CrudClient.sendAndGetClientReply(raftClient, requestMsg.serializeObjectToByteString()));
                }
                case 4 -> {
                    System.out.println("\nUpdate value");
                    System.out.print("Key: ");
                    key = stdin.next();
                    System.out.print("Value: ");
                    value = stdin.next();

                    CrudMessage requestMsg = CrudMessage.newUpdateRequest(key, value);

                    System.out.println("Response: " + CrudClient.sendAndGetClientReply(raftClient, requestMsg.serializeObjectToByteString()));
                }
                case 5 -> {
                    CrudMessage requestMsg = CrudMessage.newSizeRequest();
                    System.out.println("\nSize: " + CrudClient.sendReadOnlyAndGetClientReply(raftClient, requestMsg.serializeObjectToByteString()));
                }
                case 6 -> {
                    CrudMessage requestMsg = CrudMessage.newKeysetRequest();
                    System.out.println("\nKey-set: " + CrudClient.sendReadOnlyAndGetClientReply(raftClient, requestMsg.serializeObjectToByteString()));
                }
                case 7 -> {
                        System.exit(0);
                }
                case 8 -> {

                    System.out.print("For i: ");
                    int i = stdin.nextInt();

                    for (int j = 0; j < i; j++) {
                        CrudMessage requestMsg = CrudMessage.newCreateRequest("key" + String.valueOf(j), "value" + String.valueOf(j));

                        System.out.println("Response: " + CrudClient.sendAndGetClientReply(raftClient, requestMsg.serializeObjectToByteString()));

                    }
                }
                default -> {
                }
            }

        }
    }

}
