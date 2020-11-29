package com.ballaci.processors;

import com.ballaci.CustomSerdes;
import com.ballaci.model.OcrReadyEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.function.Function;

@Component
@Slf4j
public class CustomAggregator {


    private static final String STATE_STORE_NAME = "ocr-store";

    @Bean
    public Function<KStream<String, OcrReadyEvent>, KStream<String, OcrReadyEvent>> customaggregator() {

        return input ->
                input
                  .transform((TransformerSupplier) () -> new OcrEventTransformer(STATE_STORE_NAME), Named.as("ocr-transformer"), STATE_STORE_NAME);
    }


    @Bean
    public StoreBuilder mystore() {
        return Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore(STATE_STORE_NAME), new Serdes.StringSerde(), CustomSerdes.StoreItemSerde());
    }
}
