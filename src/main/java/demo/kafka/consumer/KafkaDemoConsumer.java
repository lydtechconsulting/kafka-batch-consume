package demo.kafka.consumer;

import java.util.concurrent.atomic.AtomicInteger;

import demo.kafka.event.DemoInboundEvent;
import demo.kafka.mapper.JsonMapper;
import demo.kafka.service.DemoService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Slf4j
@RequiredArgsConstructor
@Component
public class KafkaDemoConsumer {

    final AtomicInteger counter = new AtomicInteger();
    final DemoService demoService;

    boolean shouldError = true;

    @KafkaListener(topics = "demo-inbound-topic", groupId = "demo-consumer-group", containerFactory = "kafkaListenerContainerFactory")
    public void listen(ConsumerRecord consumerRecord) {
        String key = consumerRecord.headers().headers(KafkaHeaders.RECEIVED_MESSAGE_KEY).toString();
        String payload = consumerRecord.value().toString();
        counter.getAndIncrement();
        log.debug("Received message [" +counter.get()+ "] - key: " + key + " - payload: " + payload + " - consumerRecord: " + consumerRecord);
        DemoInboundEvent event = JsonMapper.readFromJson(payload, DemoInboundEvent.class);

        if(event.getId().equals("bravo") && shouldError) {
            // Fail the 'bravo' message the first time.  This should retry and be successfully processed.
            shouldError = false;

            // Put a breakpoint on the next line.  Release the breakpoint in the component test to write another message to the topic, then release this breakpoint.
            throw new RuntimeException("Enforced fail on 'bravo'");
        }

        demoService.process(key, event);
    }
}
