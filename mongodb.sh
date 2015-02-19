#!/bin/bash
echo "db.dropDatabase()" | mongo ycsb
bin/ycsb load mongodb -s -P workloads/workloada -threads 25 > mongodb.out 2>&1
