echo "db.dropDatabase()" | mongo ycsb
bin/ycsb load mongodb -s -P workloads/workloada -threads 1 > mongodb.out 2>&1
