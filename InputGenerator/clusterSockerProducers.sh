#!/bin/bash

echo "Starting $1 socket producers"

hosts=(2 6 8)
size=${#hosts[@]}

port=31000
end=$1

for ((i=0; i < end; i++))
do
  index=$((i % size))
  machine=${hosts[$index]}

  echo "Starting producer $i on ibm-power-$machine with port $port"
  cmd="java -cp /home/hadoop/bartnik_thesis/producer/InputGenerator-1.0-SNAPSHOT.jar de.adrian.thesis.generator.benchmark.SocketBenchmark"
  cmd+=" -n Instance$i"
  cmd+=" -p $port "
  p=" ${@:2} &"
  cmd+=$p

  echo $cmd

  # ssh ibm-power-$machine "$cmd"
  ssh -n -f ibm-power-$machine "sh -c '$cmd > /dev/null 2>&1 &'"
  port=$((++port))
done

# Ensure killing all remaining processes
