/*
    This class its based by ratis: https://github.com/apache/ratis/blob/master/ratis-examples/src/main/java/org/apache/ratis/examples/common/Constants.java
 */

package config;

import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public final class Config {

    public static final List<RaftPeer> PEERS;

    static {
        final Properties properties = new Properties();

        // path/location to access the cluster configuration
        final String clusterConf = "./src/main/resources/cluster.properties";

        // Open configuration file
        try(InputStream inputStream = new FileInputStream(clusterConf);
            Reader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
            BufferedReader bufferedReader = new BufferedReader(reader)) {
            properties.load(bufferedReader);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to load " + clusterConf, e);
        }

        final String key = "raft.server.address.list";
        final String[] addresses = Optional.ofNullable(properties.getProperty(key))
                .map(s -> s.split(","))
                .orElse(null);
        if (addresses == null || addresses.length == 0) {
            throw new IllegalArgumentException("Failed to get " + key + " from " + clusterConf);
        }

        // identify each node/server by a integer
        final String[] serverName = new String[addresses.length];
        for (int i = 0; i < addresses.length; i++) {
            serverName[i] = "node" + i;
        }

        // build cluster
        List<RaftPeer> peers = new ArrayList<>(addresses.length );
        for (int i = 0; i < addresses.length ; i++) {
            peers.add(RaftPeer.newBuilder().setId("node" + i).setAddress(addresses[i]).build());
        }

        PEERS = Collections.unmodifiableList(peers);
    }
    private Config() {
    }
    private static final UUID CLUSTER_GROUP_ID = UUID.fromString("02511d47-d67c-49a3-9011-abb3109a44c1");
    public static final RaftGroup RAFT_GROUP = RaftGroup.valueOf(
            RaftGroupId.valueOf(Config.CLUSTER_GROUP_ID), PEERS);

}
