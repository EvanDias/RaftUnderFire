#!/bin/bash
cd ../../..

java -cp target/RaftUnderFire-1.0-SNAPSHOT.jar:lib/core-0.17.0.jar:lib/htrace-core4-4.1.0-incubating.jar site.ycsb.Client -threads 10 -P config/workloada -db mapdb.ycsb.YCSBClient -s
