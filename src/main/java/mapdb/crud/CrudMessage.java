package mapdb.crud;

import lombok.Getter;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import java.io.*;

@Getter
public class CrudMessage implements Serializable {

    public enum Type {
        CREATE, READ, UPDATE, DELETE, SIZE, KEYSET, NON
    };

    private Type type = Type.NON;
    private String key = "";
    private String value = "";
    private RaftClientReply reply;

    public CrudMessage() {
    }

    public static CrudMessage newCreateRequest(String key, String value) {
        CrudMessage message = new CrudMessage();
        message.type = Type.CREATE;
        message.key = key;
        message.value = value;
        return message;
    }

    public static CrudMessage newReadRequest(String key) {
        CrudMessage message = new CrudMessage();
        message.type = Type.READ;
        message.key = key;
        return message;
    }

    public static CrudMessage newDeleteRequest(String key) {
        CrudMessage message = new CrudMessage();
        message.type = Type.DELETE;
        message.key = key;
        return message;
    }

    public static CrudMessage newUpdateRequest(String key, String value) {
        CrudMessage message = new CrudMessage();
        message.type = Type.UPDATE;
        message.key = key;
        message.value = value;
        return message;
    }

    public static CrudMessage newSizeRequest() {
        CrudMessage message = new CrudMessage();
        message.type = Type.SIZE;
        return message;
    }

    public static CrudMessage newKeysetRequest() {
        CrudMessage message = new CrudMessage();
        message.type = Type.KEYSET;
        return message;
    }







    public static CrudMessage newReadResponse(RaftClientReply reply) {
        CrudMessage message = new CrudMessage();
        message.reply = reply;
        return message;
    }

    public static CrudMessage newCreateResponse(RaftClientReply reply) {
        CrudMessage message = new CrudMessage();
        message.reply = reply;
        return message;
    }

    public ByteString serializeObjectToByteString() {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ObjectOutputStream os = new ObjectOutputStream(out);
            os.writeObject(this);
            byte[] b = out.toByteArray();

            out.close();
            os.close();

            return toByteString(b);
        } catch (IOException ex) {
            return null;
        }
    }

    public static CrudMessage deserializeByteStringToObject(ByteString bs) {
        try {
        byte[] b = bs.toByteArray();
        ByteArrayInputStream in = new ByteArrayInputStream(b);
        ObjectInputStream is = new ObjectInputStream(in);

        CrudMessage msg = (CrudMessage) is.readObject();
        in.close();
        is.close();

        return msg;

        } catch (ClassNotFoundException | IOException ex) {
            return null;
        }
    }

    public static ByteString toByteString(byte[] b) {
        return ByteString.copyFrom(b);
    }

    public static void main(String[] args)  {

        CrudMessage msg1 = newReadRequest("k1");

        System.out.println("Before serialization: " + msg1.getKey());

        ByteString bs = msg1.serializeObjectToByteString();

        System.out.println(bs);

        CrudMessage dMsg1 = deserializeByteStringToObject(bs);

        System.out.println("After deserialization: " + dMsg1.getKey());

    }



}
