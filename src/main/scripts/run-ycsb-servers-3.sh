#!/bin/bash
cd ../../..

SERVER1="java -cp target/* mapdb.RaftServer 1 ycsb"
SERVER2="java -cp target/* mapdb.RaftServer 2 ycsb"
SERVER3="java -cp target/* mapdb.RaftServer 3 ycsb"
gnome-terminal --tab --title="node0" -e "$SERVER1" --tab --title="node1" -e "$SERVER2" --tab --title="node2" -e "$SERVER3"


