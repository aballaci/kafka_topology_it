package com.ballaci.consumer;

import com.ballaci.model.OcrReadyEvent;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
public class AggregatedEventsConsumer {

    @Bean
    public java.util.function.Consumer<KStream<String, OcrReadyEvent>> aggregated() {

        return input ->
                input.foreach((key, value) -> {
                    System.out.println("Aggregated Key: " + key + " Value: " + value);
                });
    }
}
