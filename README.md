# InputGenerator for my Masterthesis

This project contains various generators, that create random or not-so-random workloads for testing my customized version of [Apache Flink](https://flink.apache.org/).
Currently, all generators generate data for RabbitMQ, but that might change in the future.

## Compile Project

* To compile the whole project, execute `./gradlew clean jar`

## Stand-alone deployment

* For a simple RabbitMQ container, run `sudo docker run -p 5001:15672 -p 5672:5672 --rm rabbitmq:3-management`
* Run the producer simply via `java -jar build/libs/InputGenerator-1.0-SNAPSHOT.jar -d 1000`

## Docker-compose

* To start the docker container with rabbitMQ, execute `sudo docker-compose up --build --rm`

## Running the FlinkJob

* Execute `./bin/flink run -p #DOP -c de.adrianbartnik.RabbitMQMapJob ../../../../../MasterthesisFlinkJobs/target/masterthesis-jobs-1.0-SNAPSHOT.jar`, where `#DOP` is the desired *degree of parallelism* for the map operator.
