#!/bin/bash
rsync -vau dist/  root@fc13:voldemort-0.81/dist
rsync -vau src/java/log4j.properties  root@fc13:voldemort-0.81/src/java/log4j.properties
rsync -vau bin/  root@fc13:voldemort-0.81/bin
