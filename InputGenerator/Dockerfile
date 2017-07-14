FROM anapsix/alpine-java

MAINTAINER Adrian

ENV host localhost
ENV port 5672

COPY ./build/libs/InputGenerator-1.0-SNAPSHOT.jar /home/output.jar

ENTRYPOINT ["/bin/sh", "-c", "exec java -jar /home/output.jar --port $port --host $host"]
