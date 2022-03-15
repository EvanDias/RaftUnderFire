package mapdb;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

import java.io.File;
import java.util.concurrent.ConcurrentMap;

public class mapdb<K,V>  {

    public static void main(String[] args) {

        hTreeMap();

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
