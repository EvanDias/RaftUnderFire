#!/bin/bash
cd ../../..

SERVER1="java -cp target/RaftUnderFire-1.0-SNAPSHOT.jar  mapdb.crud.CrudServer 1"
SERVER2="java -cp target/RaftUnderFire-1.0-SNAPSHOT.jar  mapdb.crud.CrudServer 2"
SERVER3="java -cp target/RaftUnderFire-1.0-SNAPSHOT.jar  mapdb.crud.CrudServer 3"
gnome-terminal --tab --title="node1" -e "$SERVER1" --tab --title="node2" -e "$SERVER2" --tab --title="node3" -e "$SERVER3"


