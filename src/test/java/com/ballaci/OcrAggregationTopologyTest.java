package com.ballaci;

import com.ballaci.model.OcrAggregatedEvent;
import com.ballaci.model.OcrReadyEvent;
import com.ballaci.model.StoreItem;
import com.ballaci.processors.OcrEventTransformer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;


public class OcrAggregationTopologyTest {

    private static final String STORE_NAME = "ocr-store";
    private static final String INPUT_TOPIC_NAME = "ocr-ready";
    private static final String OUTPUT_TOPIC_NAME = "ocr-aggregated";

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, OcrReadyEvent> inputTopic;
    private TestOutputTopic<String, OcrAggregatedEvent> outputTopic;
    private KeyValueStore<String, StoreItem> store;

    private Serde<String> stringSerde = new Serdes.StringSerde();
    private Serde<StoreItem> storiItemSerde = CustomSerdes.StoreItemSerde();
    private Serde<OcrReadyEvent> ocrReadySerde = CustomSerdes.OcrReadyEvent();
    private Serde<OcrAggregatedEvent> ocrAggregatedSerde = CustomSerdes.OcrAggregatedSerde();

    @BeforeEach
    public void setup() {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.addStateStore(Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore(STORE_NAME), stringSerde, storiItemSerde).withLoggingEnabled(new HashMap<>()));
        final KStream<String, OcrReadyEvent> stream = builder.stream(INPUT_TOPIC_NAME, Consumed.with(stringSerde, ocrReadySerde));
        stream.transform((TransformerSupplier) () -> new OcrEventTransformer(STORE_NAME), Named.as("ocr-transformer"), STORE_NAME).to(OUTPUT_TOPIC_NAME, Produced.with(stringSerde, ocrAggregatedSerde));

        // setup test driver
        Properties props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "ocrMessageAggregation");
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        testDriver = new TopologyTestDriver(builder.build(), props, Instant.ofEpochMilli(0L));

        // setup test topics
        inputTopic = testDriver.createInputTopic(INPUT_TOPIC_NAME, stringSerde.serializer(), new JsonSerializer<>());
        outputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC_NAME, stringSerde.deserializer(), new JsonDeserializer<>(OcrAggregatedEvent.class));

        // pre-populate store
        store = testDriver.getKeyValueStore(STORE_NAME);
    }

    @AfterEach
    public void tearDown() {
//        testDriver.close();
    }


    @Test
    public void shouldSaveMessageInStore() {
        OcrReadyEvent event = new OcrReadyEvent("fileRef", true, 1, 2);
        inputTopic.pipeInput("a", event);
        assertThat(store.get("a").getCount().intValue(), equalTo(Integer.valueOf(1)));
    }

    @Test
    public void shouldCleanStoreAfterAggregation() {
        OcrReadyEvent event1 = new OcrReadyEvent("fileRef1", true, 1, 2);
        OcrReadyEvent event2 = new OcrReadyEvent("fileRef2", true, 2, 2);
        inputTopic.pipeInput("a", event1);
        inputTopic.pipeInput("a", event2);
        assertThat(store.get("a"), equalTo(null));
    }

    @Test
    public void shouldProduceAggregatedEvent() {
        OcrReadyEvent event1 = new OcrReadyEvent("fileRef1", true, 1, 2);
        OcrReadyEvent event2 = new OcrReadyEvent("fileRef2", true, 2, 2);
        inputTopic.pipeInput("a", event1);
        inputTopic.pipeInput("a", event2);
        List<String> filerefs = Arrays.asList(new String[]{"fileRef1", "fileRef2"});
        OcrAggregatedEvent expected = new OcrAggregatedEvent(filerefs, true, 2, 2, "aggregated");
        assertThat(outputTopic.readValue(), equalTo(expected));
    }


    @Test
    public void shouldExpireIncompleteMessagesAfterTimeout() throws InterruptedException {
        OcrReadyEvent event1 = new OcrReadyEvent("fileRef1", true, 1, 2);
        inputTopic.pipeInput("a", event1);
        StoreItem storeItem = store.get("a");
        storeItem.setCreationTime(System.currentTimeMillis() - 3000);
        store.put("a", storeItem);
        testDriver.advanceWallClockTime(Duration.ofSeconds(3));
        assertThat(store.get("a"), equalTo(null));
        assertThat(outputTopic.isEmpty(), is(false));
        OcrAggregatedEvent expected = new OcrAggregatedEvent(Arrays.asList(new String[]{"fileRef1"}), false, 2, 1, "expired");
        assertThat(outputTopic.readValue(), equalTo(expected));
    }

}
