#!/bin/bash

servers=(10.0.1.85 10.0.1.86 10.0.1.87 10.0.1.88 10.0.1.89)
#servers=(10.0.1.81 10.0.1.82)

for server in ${servers[@]}; do
ssh root@$server "if [ ! -d voldemort ]; then mkdir voldemort; fi"
ssh root@$server "if [ ! -d voldemort/config ]; then mkdir voldemort/config; fi"
rsync -vau dist/  root@$server:voldemort/dist
rsync -vau config/test_cluster/  root@$server:voldemort/config/test_cluster
rsync -vau lib/  root@$server:voldemort/lib
rsync -vau src/  root@$server:voldemort/src
rsync -vau src/java/log4j.properties  root@$server:voldemort/src/java/log4j.properties
rsync -vau bin/  root@$server:voldemort/bin
done
