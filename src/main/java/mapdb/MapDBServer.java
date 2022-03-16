package mapdb;

import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

import java.util.logging.Logger;


public class MapDBServer extends BaseStateMachine {

    public DB db;
    public HTreeMap<String, String> map;
    private final Logger logger;

    public MapDBServer(String dbpath, String nameMap) {
        this.db = DBMaker.fileDB(dbpath).fileMmapEnable().make();
        this.logger = Logger.getLogger(MapDBServer.class.getName());
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
