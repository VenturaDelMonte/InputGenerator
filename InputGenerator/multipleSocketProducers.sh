#!/bin/bash

echo "Starting $1 socket producers"

port=31000

for i in $(seq 1 $1)
do
  echo "Starting producer $i on port $port"
  java -cp build/libs/InputGenerator-1.0-SNAPSHOT.jar de.adrian.thesis.generator.benchmark.SocketBenchmark -p $port "${@:2}" &
  port=$((++port))
done
