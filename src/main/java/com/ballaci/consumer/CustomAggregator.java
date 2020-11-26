package com.ballaci.consumer;

import com.ballaci.CustomSerdes;
import com.ballaci.model.OcrReadyEvent;
import com.ballaci.processors.OcrEventTransformer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
public class CustomAggregator {

    private static final Logger LOGGER = LoggerFactory.getLogger(CustomAggregator.class);

    @Autowired
    private ApplicationContext applicationContext;

    private String stateStoreName = "ocr-store";

    @Bean
    public java.util.function.Consumer<KStream<String, OcrReadyEvent>> customaggregator() {

        return input ->
                input
                        .peek((k,v) -> LOGGER.info("Peek in transformer: " + k + " Value: " + v))
                        .transform(new TransformerSupplier(){

                            @Override
                            public Transformer get() {
                                return new OcrEventTransformer(stateStoreName);
                            }
                        }, Named.as("cor-transformer"), stateStoreName)
                        .peek((k,v) -> LOGGER.info("Done Aggregation: " + k + " Value: " + v));
    }


    @Bean
    public StoreBuilder mystore() {
        return Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore(stateStoreName), new Serdes.StringSerde(), CustomSerdes.StoreItemSerde());
    }
}
