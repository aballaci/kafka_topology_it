package com.ballaci;

import com.ballaci.model.OcrReadyEvent;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

//@EmbeddedKafka(topics = { "ocr-ready", "ocr-aggregated" }, brokerProperties = { "listeners=PLAINTEXT://localhost:9092" })
@ExtendWith(SpringExtension.class)
@EmbeddedKafka(partitions = 1, topics = {"ocr-ready", "ocr-aggregated"})
@SpringBootTest(properties = "spring.autoconfigure.exclude=org.springframework.cloud.stream.test.binder.TestSupportBinderAutoConfiguration")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class KstreamsTest {

    private static final String TOPIC_OCR_READY = "ocr-ready";
    private static final String TOPIC_OCR_AGG = "ocr-aggregated";

    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    // Autowire the kafka broker registered via @EmbeddedKafka
    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    @Before
    void setUp() {}

    @After
    void tearDown() {}

    @Test
    public void test() throws InterruptedException, ExecutionException {
        Map<String, Object> produserConfig = new HashMap<>(KafkaTestUtils.producerProps(embeddedKafka));
        Producer<String, OcrReadyEvent> producer = new DefaultKafkaProducerFactory<>(produserConfig, new StringSerializer(), new JsonSerializer<OcrReadyEvent>()).createProducer();


        producer.send(new ProducerRecord<String, OcrReadyEvent>(TOPIC_OCR_READY, "doc1", new OcrReadyEvent("ref1", true))).get();
        producer.send(new ProducerRecord<String, OcrReadyEvent>(TOPIC_OCR_READY, "doc1", new OcrReadyEvent("ref2", true))).get();
        producer.send(new ProducerRecord<String, OcrReadyEvent>(TOPIC_OCR_READY, "doc1", new OcrReadyEvent("ref3", true))).get();

        Map<String, Object> configs = new HashMap<>(KafkaTestUtils.consumerProps("consumer", "false", embeddedKafka));
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        Consumer<String, OcrReadyEvent> consumer = new DefaultKafkaConsumerFactory<>(configs, new StringDeserializer(), new JsonDeserializer<>(OcrReadyEvent.class)).createConsumer();
        consumer.subscribe(Collections.singleton(TOPIC_OCR_AGG));
        System.out.println("In the test...");
        Awaitility.await().timeout(10L, TimeUnit.SECONDS).untilAsserted(() -> {
            ConsumerRecord<String, OcrReadyEvent> message1 = KafkaTestUtils.getSingleRecord(consumer, TOPIC_OCR_AGG);
            System.out.println(message1.toString());
            assertThat(message1).isNotNull();
        });
    }
}
