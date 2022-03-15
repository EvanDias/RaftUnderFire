package mapdb;

import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

import java.io.File;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;

public class MapDBServer<K, V> extends BaseStateMachine {

    DB db;
    HTreeMap<String, Long> map;
    private Logger logger;

    String[] requestType = {"PUT", "GET", "REMOVE", "SIZE", "KEYSET"};

    public MapDBServer(String dbpath, String nameMap) {
        this.db = DBMaker.fileDB(dbpath).fileMmapEnable().make();
        this.logger = Logger.getLogger(MapDBServer.class.getName());
        this.map = db.hashMap(nameMap)
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.LONG)
                .create();
    }

    public static void main(String[] args) {


    }

    public enum MapRequestType {
        PUT, GET, SIZE, REMOVE, KEYSET;
    }

    public CompletableFuture<Message> appExecuteOrdered(HTreeMap<String, Long> map, Message request, TransactionContext trx) {
        //K key = null;
        //V value = null;
        //boolean hasReply = false;

        // get entry/command of the client
        final RaftProtos.LogEntryProto entry = trx.getLogEntry();

        // check if the command is valid
        String logData = entry.getStateMachineLogEntry().getLogData()
                .toString(Charset.defaultCharset());
        if(!Arrays.asList(requestType).contains(logData)) // Is not a default request type
            return CompletableFuture.completedFuture(
                    Message.valueOf("Invalid Command (Request Type)"));

        //update the last applied term and index
        final long index = entry.getIndex();
        updateLastAppliedTermIndex(entry.getTerm(), index);

        String msg = request.getContent().toString(Charset.defaultCharset());
        switch (msg) {
            case "PUT":



                break;
                case "GET":
                    break;

                case "REMOVE":

                    break;

                case "SIZE":

                    break;

                case "KEYSET":
                    break;

                    default:
                        return CompletableFuture.completedFuture(
                                Message.valueOf("Invalid Command"));

            }


        return CompletableFuture.completedFuture(
                Message.valueOf("yyyyyyyyyy"));
    }

    public void hTreeMap() {

        final String dbpath = "src/main/java/mapdb/files/serverdb.db";
        new MapDBServer(dbpath, "name_of_map");

        File f = new File(dbpath);
        if(f.delete()) {
            System.out.println("DB already existed, so we deleted them");
        }

        this.map.put("key1", 123L);
        this.map.put("key2", 1234L);
        this.map.put("key3", 1235L);
        this.map.put("key4", 1236L);
        this.map.put("key5", 1237L);
        this.map.put("key6", 1238L);

        System.out.println("KeySet: " + map.keySet());
        System.out.println("H size: " + map.size());


        db.close();
    }


}
