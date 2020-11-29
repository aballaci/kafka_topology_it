package com.ballaci.consumer;

import com.ballaci.CustomSerdes;
import com.ballaci.model.OcrReadyEvent;
import com.ballaci.processors.OcrEventTransformer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.function.Function;

@Component
public class CustomAggregator {

    private static final Logger LOGGER = LoggerFactory.getLogger(CustomAggregator.class);

    private String stateStoreName = "ocr-store";

    @Bean
    public Function<KStream<String, OcrReadyEvent>, KStream<String, OcrReadyEvent>> customaggregator() {

        return input ->
                input
                        .transform((TransformerSupplier) () -> new OcrEventTransformer(stateStoreName), Named.as("ocr-transformer"), stateStoreName);
    }


    @Bean
    public StoreBuilder mystore() {
        return Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore(stateStoreName), new Serdes.StringSerde(), CustomSerdes.StoreItemSerde());
    }
}
