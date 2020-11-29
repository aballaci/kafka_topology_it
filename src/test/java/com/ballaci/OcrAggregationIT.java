package com.ballaci;

import com.ballaci.model.OcrAggregatedEvent;
import com.ballaci.model.OcrReadyEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
@Slf4j
@ExtendWith(SpringExtension.class)
@EmbeddedKafka(partitions = 1, topics = {"ocr-ready", "ocr-aggregated"})
@SpringBootTest(properties = "spring.autoconfigure.exclude=org.springframework.cloud.stream.test.binder.TestSupportBinderAutoConfiguration")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class OcrAggregationIT {

    private static final String TOPIC_OCR_READY = "ocr-ready";
    private static final String TOPIC_OCR_AGG = "ocr-aggregated";

    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    // Autowire the kafka broker registered via @EmbeddedKafka
    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    @Before
    void setUp() {
    }

    @After
    void tearDown() {
    }

    @Test
    public void test() throws InterruptedException, ExecutionException {
        Map<String, Object> produserConfig = new HashMap<>(KafkaTestUtils.producerProps(embeddedKafka));
        Producer<String, OcrReadyEvent> producer = new DefaultKafkaProducerFactory<>(produserConfig, new StringSerializer(), new JsonSerializer<OcrReadyEvent>()).createProducer();


        producer.send(new ProducerRecord<String, OcrReadyEvent>(TOPIC_OCR_READY, "doc3", new OcrReadyEvent("doc3-ref1", true, 1, 1))).get();

        producer.send(new ProducerRecord<String, OcrReadyEvent>(TOPIC_OCR_READY, "doc4", new OcrReadyEvent("doc4-ref1", true, 1, 10))).get();
        producer.send(new ProducerRecord<String, OcrReadyEvent>(TOPIC_OCR_READY, "doc4", new OcrReadyEvent("doc4-ref2", true, 2, 10))).get();

        producer.send(new ProducerRecord<String, OcrReadyEvent>(TOPIC_OCR_READY, "doc1", new OcrReadyEvent("doc1-ref1", true, 1, 3))).get();
        producer.send(new ProducerRecord<String, OcrReadyEvent>(TOPIC_OCR_READY, "doc1", new OcrReadyEvent("doc1-ref2", true, 2, 3))).get();
        producer.send(new ProducerRecord<String, OcrReadyEvent>(TOPIC_OCR_READY, "doc1", new OcrReadyEvent("doc1-ref3", true, 3, 3))).get();

        producer.send(new ProducerRecord<String, OcrReadyEvent>(TOPIC_OCR_READY, "doc2", new OcrReadyEvent("doc2-ref1", true, 1, 3))).get();
        producer.send(new ProducerRecord<String, OcrReadyEvent>(TOPIC_OCR_READY, "doc2", new OcrReadyEvent("doc2-ref2", true, 2, 3))).get();
        producer.send(new ProducerRecord<String, OcrReadyEvent>(TOPIC_OCR_READY, "doc2", new OcrReadyEvent("doc2-ref3", true, 3, 3))).get();

        Map<String, Object> configs = new HashMap<>(KafkaTestUtils.consumerProps("my-test-consumer", "true", embeddedKafka));
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        Consumer<String, OcrAggregatedEvent> consumer = new DefaultKafkaConsumerFactory<>(configs, new StringDeserializer(), new JsonDeserializer<>(OcrAggregatedEvent.class)).createConsumer();
        consumer.subscribe(Collections.singleton(TOPIC_OCR_AGG));
        List<OcrAggregatedEvent> resultList = new ArrayList<>();
        Awaitility.await().atMost(5L, TimeUnit.SECONDS).untilAsserted(() -> {
            ConsumerRecords<String, OcrAggregatedEvent> messages = consumer.poll(Duration.ofMillis(500));
            messages.forEach(m -> {
                log.info(m.value().toString());
                resultList.add(m.value());
            });
            assertThat(resultList).isNotEmpty();
            assertThat(resultList.size()).isEqualTo(4);
            assertThat(resultList.get(3).isStatus()).isFalse();
            assertThat(resultList.get(3).getMessage()).isEqualTo("expired");

        });
    }

}
