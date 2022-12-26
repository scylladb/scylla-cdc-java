FROM adoptopenjdk/openjdk11

RUN \
  apt-get update \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

RUN mkdir /scylla-kafka

WORKDIR  /scylla-kafka

COPY ./target/scylla-kafka-connector-*-SNAPSHOT.jar /scylla-kafka/scylla-kafka-connector.jar
COPY src/main/resources/scylla_config.yml /scylla-kafka/scylla_config.yml
COPY ./src/main/scripts/setup_env.sh /scylla-kafka/
COPY ./src/main/scripts/run.sh /scylla-kafka/

RUN chmod +x /scylla-kafka/setup_env.sh
RUN chmod +x /scylla-kafka/run.sh
RUN chmod +x /scylla-kafka/scylla_config.yml

CMD ["/bin/bash", "-c", "/scylla-kafka/run.sh"]






