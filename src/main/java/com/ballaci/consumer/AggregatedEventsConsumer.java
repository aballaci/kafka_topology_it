package com.ballaci.consumer;

import com.ballaci.CustomSerdes;
import com.ballaci.model.OcrReadyEvent;
import com.ballaci.model.StoreItem;
import com.ballaci.processors.OcrEventTransformer;
import com.ballaci.processors.OcrTransformerSuplier;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Component;

@Component
public class AggregatedEventsConsumer {

    @Autowired
    private ApplicationContext applicationContext;

    private String stateStoreName = "ocr-store";

    @Bean
    public java.util.function.Consumer<KStream<String, OcrReadyEvent>> aggregated() {

        return input ->
                input
                        .transform(new TransformerSupplier(){

                            @Override
                            public Transformer get() {
                                return new OcrEventTransformer(stateStoreName);
                            }
                        }, Named.as("cor-transformer"), stateStoreName)
                        .foreach((key, value) -> {
                    System.out.println("Aggregated Key: " + key + " Value: " + value);
                });
    }


    @Bean
    public StoreBuilder mystore() {
        return Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore(stateStoreName), new Serdes.StringSerde(), CustomSerdes.StoreItemSerde());
    }
}
