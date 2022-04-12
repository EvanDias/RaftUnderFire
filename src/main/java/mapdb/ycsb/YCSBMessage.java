package mapdb.ycsb;

import lombok.Getter;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import site.ycsb.ByteIterator;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.*;

@Getter
public class YCSBMessage implements Serializable {

    //private static final long serialVersionUID = 6198684730704708506L;

    public enum Type {
        CREATE, READ, SCAN, UPDATE, DELETE, SIZE, KEYSET, NON, ERROR
    };

    // That enum was created to replace "import site.ycsb.Status", because this class cannot
    // be serialized (do not "implements Serializable") so after the deserialization (enum)
    // we can associate the "ReplyStatus enum" with the "YCSB Status"
    public enum ReplyStatus {
        OK, ERROR, NOT_FOUND, NOT_IMPLEMENTED, UNEXPECTED_STATE, BAD_REQUEST,
        FORBIDDEN, SERVICE_UNAVAILABLE, BATCHED_OK
    };

    private Type type = Type.NON;
    private String key = "";
    private String value;
    private Set<String> fields;
    private ReplyStatus status;
    private Map<String, ByteIterator> results;
    private String errorMsg = "";
    private String response = "";

    private YCSBMessage() {
        //super();
    }

    public static YCSBMessage newCreateRequest(String key, String value) {
        YCSBMessage message = new YCSBMessage();
        message.type = Type.CREATE;
        message.key = key;
        message.value = value;
        return message;
    }

    public static YCSBMessage newReadRequest(String key, Set<String> fields, Map<String, ByteIterator> results) {
        YCSBMessage message = new YCSBMessage();
        message.type = Type.READ;
        message.key = key;
        message.fields = fields;
        message.results = results;
        return message;
    }

    public static YCSBMessage newUpdateRequest(String key, String value) {
        YCSBMessage message = new YCSBMessage();
        message.type = Type.UPDATE;
        message.key = key;
        message.value = value;
        return message;
    }



    public static YCSBMessage newReply(String response, ReplyStatus result) {
        YCSBMessage message = new YCSBMessage();
        message.response = response;
        message.status = result;
        return message;
    }


    public static YCSBMessage newErrorMessage(String errorMsg) {
        YCSBMessage message = new YCSBMessage();
        message.errorMsg = errorMsg;
        return message;
    }


    /*
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("(").append(type).append(",").append(entity).append(",");
        sb.append(table).append(",").append(key).append(",").append(values).append(")");
        return sb.toString();
    }*/


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
            ex.printStackTrace();
            return null;
        }
    }


    public static YCSBMessage deserializeByteStringToObject(ByteString bs) {
        try {
            byte[] b = bs.toByteArray();
            ByteArrayInputStream in = new ByteArrayInputStream(b);
            ObjectInputStream is = new ObjectInputStream(in);

            YCSBMessage msg = (YCSBMessage) is.readObject();
            in.close();
            is.close();

            return msg;

        } catch (ClassNotFoundException | IOException ex) {
            ex.printStackTrace();
            return null;
        }
    }

    public static ByteString toByteString(byte[] b) {
        return ByteString.copyFrom(b);
    }

}
