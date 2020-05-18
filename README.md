# InputGenerator for my Masterthesis

This project contains various generators, that create random or not-so-random workloads for testing my customized version of [Apache Flink](https://flink.apache.org/).
Currently, all generators generate data for RabbitMQ, but that might change in the future.

## Compile Project

* To compile the whole project, execute `./gradlew clean jar`

## Stand-alone deployment

* Run the producer simply via `java -jar build/libs/InputGenerator-1.0-SNAPSHOT.jar -d 1000`


## Running the FlinkJob

* Execute `./bin/flink run -p #DOP -c de.adrianbartnik.RabbitMQMapJob ../../../../../MasterthesisFlinkJobs/target/masterthesis-jobs-1.0-SNAPSHOT.jar`, where `#DOP` is the desired *degree of parallelism* for the map operator.
