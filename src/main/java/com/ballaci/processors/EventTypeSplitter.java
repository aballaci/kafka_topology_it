package com.ballaci.processors;

import com.ballaci.model.Event;
import com.ballaci.model.EventKey;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;

import java.util.function.BiConsumer;
import java.util.function.Function;

//@Service
public class EventTypeSplitter {

    private static final String TYPE_1 = "type1";

    @Bean
    public Function<KStream<EventKey, Event>, KStream<EventKey, Event>> splitStream() {

        Predicate<EventKey, Event> isOfType1 = (k, v) -> TYPE_1.equals(v.getType());
        ForeachAction<EventKey, Event> printEventValue = (k,v) -> System.out.println(v);
        return input -> input
                .peek(printEventValue)
                .filter(isOfType1);
    }
}