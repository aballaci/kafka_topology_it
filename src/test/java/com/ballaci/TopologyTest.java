package com.ballaci;

import com.ballaci.model.OcrReadyEvent;
import com.ballaci.model.StoreItem;
import com.ballaci.processors.OcrEventProcessor;
import com.ballaci.processors.OcrEventTransformer;
import com.ballaci.processors.OcrProcessorSuplier;
import com.ballaci.processors.OcrTransformerSuplier;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(SpringExtension.class)
@EmbeddedKafka(partitions = 1, topics = {"ocr-ready", "ocr-aggregated"})
@SpringBootTest(properties = "spring.autoconfigure.exclude=org.springframework.cloud.stream.test.binder.TestSupportBinderAutoConfiguration")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class TopologyTest {

    private static final String TOPIC_OCR_READY = "ocr-ready";
    private static final String TOPIC_OCR_AGG = "ocr-aggregated";

    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    // Autowire the kafka broker registered via @EmbeddedKafka
    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    @Value("${spring.embedded.kafka.brokers}")
    private String brokerAddresses;

    @Before
    void setUp() {}

    @After
    void tearDown() {}

    @Test
    public void test() throws InterruptedException, ExecutionException {
        Map<String, Object> producerConfig = new HashMap<>(KafkaTestUtils.producerProps(embeddedKafka));
        Producer<String, OcrReadyEvent> producer = new DefaultKafkaProducerFactory<>(producerConfig, new StringSerializer(), new JsonSerializer<OcrReadyEvent>()).createProducer();



        Map<String, Object> configs = new HashMap<>(KafkaTestUtils.consumerProps("my-test-consumer", "true", embeddedKafka));
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        Consumer<String, OcrReadyEvent> consumer = new DefaultKafkaConsumerFactory<>(configs, new StringDeserializer(), new JsonDeserializer<>(OcrReadyEvent.class)).createConsumer();
        consumer.subscribe(Collections.singleton(TOPIC_OCR_AGG));
        String ocrStateStore = "ocr-store";

        StreamsConfig streamsConfig = new StreamsConfig(getProperties());

        StreamsBuilder builder = new StreamsBuilder();

              StoreBuilder<KeyValueStore<String,StoreItem>> keyValueStoreBuilder =
             Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore(ocrStateStore),
                               Serdes.String(),
                      CustomSerdes.StoreItemSerde());
      // register store
      builder.addStateStore(keyValueStoreBuilder);




//        .to(new Serdes.StringSerde(), CustomSerdes.OcrReadyEvent(), "stock-performance");

//                .print(Printed.<String, OcrReadyEvent>toSysOut().withLabel("Aggregated OCR Events"));

        //Uncomment this line and comment out the line above for writing to a topic


        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsConfig);



        producer.send(new ProducerRecord<String, OcrReadyEvent>(TOPIC_OCR_READY, "doc1", new OcrReadyEvent("ref1", true))).get();
        producer.send(new ProducerRecord<String, OcrReadyEvent>(TOPIC_OCR_READY, "doc1", new OcrReadyEvent("ref2", true))).get();
        producer.send(new ProducerRecord<String, OcrReadyEvent>(TOPIC_OCR_READY, "doc1", new OcrReadyEvent("ref3", true))).get();

        System.out.println("Ocr KStream/Process API App Started");
        kafkaStreams.cleanUp();
        kafkaStreams.start();
        Thread.sleep(70000);
        System.out.println("Shutting down the Ocr KStream/Process API Analysis App now");
        kafkaStreams.close();

        System.out.println("In the test...");
        Awaitility.await().timeout(5L, TimeUnit.SECONDS).untilAsserted(() -> {
//            ConsumerRecord<String, OcrReadyEvent> message1 = KafkaTestUtils.getSingleRecord(consumer, TOPIC_OCR_AGG);
            ConsumerRecords<String, OcrReadyEvent> messages = consumer.poll(Duration.ofMillis(100));
            messages.forEach(m -> System.out.println(m.toString()));
            assertThat(messages).isNotNull();
        });
    }

    private Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "ks-papi-ocr-processor-client");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "ks-papi-ocr-processor-group");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ks-ocr-processor-appid");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, this.brokerAddresses);
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, CustomSerdes.OcrReadyEvent().getClass().getName());
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        return props;
    }
}
