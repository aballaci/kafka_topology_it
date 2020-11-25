package com.ballaci.processors;

import com.ballaci.model.Event;
import com.ballaci.model.EventKey;
import com.ballaci.model.OcrReadyEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import java.util.function.Function;

//@Component
public class OcrReadyAggregator {

    @Bean
    public Function<KStream<String, OcrReadyEvent>, KStream<String, OcrReadyEvent>> ocrAggregator() {


        ForeachAction<String, OcrReadyEvent> printEventValue = (k,v) -> System.out.println("#########" + k + ":" + v.toString());
        return input -> input
                .peek(printEventValue);

//                .groupBy(
//                        (s, domainEvent) -> domainEvent.boardUuid, Serialized.with(s, domainEventSerde))
//                .aggregate(
//                        String::new,
//                        (s, domainEvent, board) -> board.concat(domainEvent.eventType),
//                        Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("test-events-snapshots").withKeySerde(Serdes.String()).
//                                withValueSerde(Serdes.String())
//                );
    }


}