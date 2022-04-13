#!/bin/bash
cd ../../..

SERVER1="java -cp target/RaftUnderFire-1.0-SNAPSHOT.jar mapdb.RaftServer 1 crud"
SERVER2="java -cp target/RaftUnderFire-1.0-SNAPSHOT.jar mapdb.RaftServer 2 crud"
SERVER3="java -cp target/RaftUnderFire-1.0-SNAPSHOT.jar mapdb.RaftServer 3 crud"
gnome-terminal --tab --title="node1" -e "$SERVER1" --tab --title="node2" -e "$SERVER2" --tab --title="node3" -e "$SERVER3"


