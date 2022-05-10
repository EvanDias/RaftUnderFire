#!/bin/bash
cd ../../..

java -cp target/*:lib/* site.ycsb.Client -threads 10 -P config/workloada -p status.interval=1 -db mapdb.ycsb.YCSBClient -s |& tee bench.txt
sed -n '/sec:/p' bench.txt > bench2.txt