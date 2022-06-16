#!/bin/bash
cd ../../..

SERVER1="java -cp target/* mapdb.RaftServer 1 ycsb"
SERVER2="java -cp target/* mapdb.RaftServer 2 ycsb"
SERVER3="java -cp target/* mapdb.RaftServer 3 ycsb"
SERVER4="java -cp target/* mapdb.RaftServer 4 ycsb"
SERVER5="java -cp target/* mapdb.RaftServer 5 ycsb"
gnome-terminal --tab --title="node1" -e "$SERVER1" --tab --title="node2" -e "$SERVER2" --tab --title="node3" -e "$SERVER3" --tab --title="node4" -e "$SERVER4" --tab --title="node5" -e "$SERVER5"


