#!/bin/bash
cd ../../..

java -cp target/*:lib/* site.ycsb.Client -threads 10 -P config/workloada -p status.interval=1 -db mapdb.ycsb.YCSBClient -s
