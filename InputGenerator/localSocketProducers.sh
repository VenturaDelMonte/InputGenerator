#!/bin/bash

echo "Starting $1 socket producers"

port=31000
debuggingPort=30000

rm -rf logs

for i in $(seq 1 $1)
do
  echo "Starting producer $i on port $port"
  java -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=$debuggingPort -cp build/libs/InputGenerator-1.0-SNAPSHOT.jar de.adrian.thesis.generator.benchmark.javaio.SocketBenchmark -n "Instance$i" -p $port "${@:2}" &

  port=$((++port))
  debuggingPort=$((++debuggingPort))
done
