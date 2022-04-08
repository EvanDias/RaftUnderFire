package mapdb;

import org.mapdb.*;
import java.util.concurrent.ConcurrentMap;
import java.io.File;

public class mapdbHelloWorld {

    public static void main(String[] args) {

        // simpledb();

        //hTreeMap();

        String node = "node3";
        System.out.println(node);

        DB db = DBMaker.fileDB("/home/evan/Desktop/codesrcs/git/RaftUnderFire/src/main/java/mapdb/files/" + node + ".db").fileMmapEnable().make();

        HTreeMap<String, String> map = db.hashMap("map"+node)
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.STRING)
                .createOrOpen();


        int size = map.size();

        System.out.println("Size hmap: " + size);
        System.out.println("KeySet: " + map.keySet());

        db.close();
        map.close();

    }

    public static void simpledb() {
        DB db = DBMaker.fileDB("src/main/java/mapdb/files/file.db").fileMmapEnable().make();

        ConcurrentMap<String, String> map = db
                .hashMap("map", Serializer.STRING, Serializer.STRING)
                .createOrOpen();

        map.put("something", "blabla");

        System.out.println(map.get("something"));

        db.close();
    }

    public static void hTreeMap() {

        String dbpath = "src/main/java/mapdb/files/htreemapfile.db";

        File f = new File(dbpath);
        if(f.delete())
            System.out.println("DB already existed, so we deleted them");

        DB db = DBMaker.fileDB(dbpath).fileMmapEnable().make();

        HTreeMap<String, Long> map = db.hashMap("name_of_map")
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.LONG)
                .create();

        map.put("key1", 123L);
        map.put("key2", 1234L);
        map.put("key3", 1235L);
        map.put("key4", 1236L);
        map.put("key5", 1237L);
        map.put("key6", 1238L);

        System.out.println("KeySet: " + map.keySet());
        System.out.println("H size: " + map.size());


        db.close();
    }
}
