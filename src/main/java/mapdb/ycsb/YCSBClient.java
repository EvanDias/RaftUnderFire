package mapdb.ycsb;

import lombok.Getter;
import lombok.SneakyThrows;
import mapdb.crud.CrudClient;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;

import java.io.IOException;
import java.util.*;


public class YCSBClient extends DB {

    @Getter
    RaftClient raftClient = CrudClient.buildClient();

    public YCSBClient() {
    }

    @Override
    public void init() throws DBException {
        super.init();
    }

    /**
     * @param table - The name of the table
     * @param key - The record key of the record to read.
     * @param fields - The list of fields to read, or null for all of them (will be only one field because it's a KVs)
     * @param result - A HashMap of field/value pairs for the result - IGNORE
     * @return - The result of the operation.
     */

    @SneakyThrows
    @Override
    public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {

        HashMap<String, byte[]> results = new HashMap<>();

        YCSBMessage request = YCSBMessage.newReadRequest(table, key, fields, result);

        YCSBMessage reply = CrudClient.sendReadOnlyAndGetClientReply2(raftClient, request.serializeObjectToByteString());

        System.out.println("key:" + reply.getKey() + ", value: " + reply.getResponse());

        return replyStatusToStatus(reply.getStatus());
    }

    @SneakyThrows
    @Override
    public Status insert(String s, String key, Map<String, ByteIterator> values) {

        Iterator<String> keys = values.keySet().iterator();

        Map<String, ByteIterator> map = new HashMap<>();

        while (keys.hasNext()) {
            String field = keys.next(); // field and value
            map.put(keys.next(), values.get(field));
        }

        YCSBMessage request = YCSBMessage.newCreateRequest(key, "testu");

        YCSBMessage reply = CrudClient.sendAndGetClientReply2(raftClient, request.serializeObjectToByteString());

        return null;
    }

    @Override
    public Status update(String s, String s1, Map<String, ByteIterator> map) {
        return Status.NOT_IMPLEMENTED;
    }


    @Override
    public Status scan(String s, String s1, int i, Set<String> set, Vector<HashMap<String, ByteIterator>> vector) {
        return Status.NOT_IMPLEMENTED;
    }

    @Override
    public Status delete(String s, String s1) {
        return Status.NOT_IMPLEMENTED;
    }


    /*
    public static Status read2(String table, String key, Set<String> fields, Map<String, ByteIterator> result, RaftClient raftClient) throws IOException {

        HashMap<String, byte[]> results = new HashMap<>();

        YCSBMessage request = YCSBMessage.newReadRequest(table, key, fields, result);

        YCSBMessage reply = CrudClient.sendReadOnlyAndGetClientReply2(raftClient, request.serializeObjectToByteString());

        System.out.println("key:" + request.getKey() + ", value: " + reply.getResponse());

        Status r = replyStatusToStatus(reply.getStatus());

        assert r != null;
        System.out.println(r.toString());

        return r;
    }*/

    public static Status insert2(String s, String key, Map<String, ByteIterator> values, RaftClient raftClient) throws IOException {

        Iterator<String> keys = values.keySet().iterator();

        Map<String, ByteIterator> map = new HashMap<>();

        while (keys.hasNext()) {
            String field = keys.next();
            map.put(keys.next(), values.get(field));
        }

        YCSBMessage request = YCSBMessage.newCreateRequest(key, "testi");

        YCSBMessage reply = CrudClient.sendAndGetClientReply2(raftClient, request.serializeObjectToByteString());

        Status r = replyStatusToStatus(reply.getStatus());

        assert r != null;
        System.out.println(r.toString());

        return r;
    }

    public static Status replyStatusToStatus(YCSBMessage.ReplyStatus status) {

        switch (status) {
            case OK:
                return Status.OK;
            case ERROR:
                return Status.ERROR;
            case NOT_IMPLEMENTED:
                return Status.NOT_IMPLEMENTED;
            case BAD_REQUEST:
                return Status.BAD_REQUEST;
            case SERVICE_UNAVAILABLE:
                return Status.SERVICE_UNAVAILABLE;
            default:
        }

        return null;
    }


    public static void main(String[] args) throws IOException {
        YCSBClient client = new YCSBClient();

        //Status reply = read2("table", "k2", null, null, client.getRaftClient());
        //Status reply2 = read2("table", "k2", null, null, client.getRaftClient());
        //Status reply3 = read2("table", "k3", null, null, client.getRaftClient());

        Map<String, ByteIterator> map = new HashMap<>();

        Status reply4 = insert2("table", "k2", map, client.getRaftClient());


        //Status reply = read2("table", "k2", null, null, client.getRaftClient());

    }
}
