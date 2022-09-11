# Kafka Spring Boot Batch Consume Demo Project

Spring Boot application demonstrating batch consume behaviour when a message throws an exception.

This repo accompanies the article [Kafka Message Batch Consumer Retry](https://medium.com/lydtech-consulting/kafka-message-batch-consumer-retry-8e49bdb39f5c).

## Component Tests

The tests demonstrate sending a batch of events to a dockerised Kafka that are then consumed as a batch by the dockerised application.

A message in the middle of the batch throws an exception causing the batch consume to fail, demonstrating the resulting behaviour.

For more on the component tests see: https://github.com/lydtechconsulting/component-test-framework

Build Spring Boot application jar:
```
mvn clean install
```

Build Docker container:
```
docker build -t ct/kafka-batch-consume:latest .
```

Run tests:
```
mvn test -Pcomponent
```

Run tests leaving containers up:
```
mvn test -Pcomponent -Dcontainers.stayup
```

Manual clean up (if left containers up):
```
docker rm -f $(docker ps -aq)
```

View messages on __consumer_offsets topic:
```
docker exec -it ct-kafka  /bin/sh /usr/bin/kafka-console-consumer  --formatter "kafka.coordinator.group.GroupMetadataManager\$OffsetsMessageFormatter" --bootstrap-server localhost:9092 --topic __consumer_offsets --from-beginning
```
