package mapdb;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

public class MapDB {

    public DB db;
    public HTreeMap<String, String> map;

    public MapDB(String dbpath, String nameMap) {
        this.db = DBMaker.fileDB(dbpath).fileMmapEnable().transactionEnable().make();
        this.map = db.hashMap(nameMap)
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.STRING)
                .createOrOpen();
    }

    public void closedb() {
        this.db.close();
    }

    public void closemap() {
        this.db.close();
    }

    public void putValue(String key, String value) {
        this.map.put(key,value);
    }

    public void updateValue(String key, String value) {
        if (this.map.containsKey(key))
            this.map.replace(key,value);
        else
            putValue(key,value);
    }

    public void deleteValue(String key) {
        this.map.remove(key);
    }

    public String getValue(String key) {
        return this.map.get(key);
    }

    public String getKeySet() {
        return this.map.keySet().toString();
    }

    public String getSize() {
        return String.valueOf(this.map.getSize());
    }

    /*
    public void hTreeMap() {

        final String dbpath = "src/main/java/mapdb/files/serverdb.db";
        new MapDBServer(dbpath, "name_of_map");

        File f = new File(dbpath);
        if(f.delete()) {
            System.out.println("DB already existed, so we deleted them");
        }

        db.close();
    }
    */

}
