package testkvs;

import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public final class Config {

    public static final List<RaftPeer> PEERS;

    static {
        List<RaftPeer> peers = new ArrayList<>(3);
        peers.add(RaftPeer.newBuilder().setId("node1").setAddress("127.0.0.1:6000").build());
        peers.add(RaftPeer.newBuilder().setId("node2").setAddress("127.0.0.1:6001").build());
        peers.add(RaftPeer.newBuilder().setId("node3").setAddress("127.0.0.1:6002").build());

        PEERS = Collections.unmodifiableList(peers);
    }

    private Config() {
    }

    private static final UUID CLUSTER_GROUP_ID = UUID.fromString("02511d47-d67c-49a3-9011-abb3109a44c1");

    public static final RaftGroup RAFT_GROUP = RaftGroup.valueOf(
            RaftGroupId.valueOf(Config.CLUSTER_GROUP_ID), PEERS);

}
