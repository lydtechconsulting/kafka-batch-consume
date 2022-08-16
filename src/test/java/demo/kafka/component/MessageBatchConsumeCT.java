package demo.kafka.component;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import dev.lydtech.component.framework.client.kafka.KafkaClient;
import dev.lydtech.component.framework.extension.TestContainersSetupExtension;
import dev.lydtech.component.framework.mapper.JsonMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static demo.kafka.util.TestEventData.INBOUND_DATA;
import static demo.kafka.util.TestEventData.buildDemoInboundEvent;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

@Slf4j
@ExtendWith(TestContainersSetupExtension.class)
public class MessageBatchConsumeCT {

    private static final String GROUP_ID = "MessageBatchConsumeCT";

    private Consumer consumer;

    @BeforeEach
    public void setup() {
        consumer = KafkaClient.getInstance().createConsumer(GROUP_ID, "demo-outbound-topic");

        // Clear the topic.
        consumer.poll(Duration.ofSeconds(1));
    }

    @AfterEach
    public void tearDown() {
        consumer.close();
    }

    /**
     * Send in three messages, 'alpha', 'bravo', and 'charlie'.
     *
     * The producer has a linger.ms configured and sends the messages asynchronously.  This ensures they are sent in a single batch.
     *
     * The message 'bravo' is hardcoded to throw an exception in the consumer the first time.
     *
     * The batch is redelivered, but starting from 'bravo'.  'alpha' is not redelivered.
     *
     * Optionally use breakpoints in the test and in the consumer to add a further event, 'delta', to the topic, before the batch is retried.
     *
     * This demonstrates that the new batch can contain more messages, and not just the 'bravo' and 'charlie' messages that had not completed processing.
     */
    @Test
    public void testBatchConsume() throws Exception {
        Properties additionalConfig = new Properties();
        additionalConfig.put(ProducerConfig.LINGER_MS_CONFIG, 100);
        KafkaProducer producer = KafkaClient.getInstance().createProducer(additionalConfig);

        KafkaClient.getInstance().sendMessageAsync(producer, "demo-inbound-topic", UUID.randomUUID().toString(), JsonMapper.writeToJson(buildDemoInboundEvent("alpha")));
        KafkaClient.getInstance().sendMessageAsync(producer, "demo-inbound-topic", UUID.randomUUID().toString(), JsonMapper.writeToJson(buildDemoInboundEvent("bravo")));
        KafkaClient.getInstance().sendMessageAsync(producer, "demo-inbound-topic", UUID.randomUUID().toString(), JsonMapper.writeToJson(buildDemoInboundEvent("charlie")));

        // Put a breakpoint on the next line, and release once hit the breakpoint in the KafkaDemoConsumer.
        KafkaClient.getInstance().sendMessageAsync(producer, "demo-inbound-topic", UUID.randomUUID().toString(), JsonMapper.writeToJson(buildDemoInboundEvent("delta")));

        List<ConsumerRecord<String, String>> outboundEvents = KafkaClient.getInstance().consumeAndAssert("testFlow", consumer, 4, 2);
        outboundEvents.stream().forEach(outboundEvent -> assertThat(outboundEvent.value(), containsString(INBOUND_DATA)));
    }
}
