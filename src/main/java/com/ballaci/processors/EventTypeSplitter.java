package com.ballaci.processors;

import com.ballaci.model.Event;
import com.ballaci.model.EventKey;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;

import java.util.function.Function;

@Service
public class EventTypeSplitter {

    private static final String TYPE_1 = "type1";

    @Bean
    public Function<KStream<EventKey, Event>, KStream<EventKey, Event>> splitStream() {

        Predicate<EventKey, Event> isType1 = (k, v) -> TYPE_1.equals(v.getType());
        return input -> input
                .peek(new ForeachAction<EventKey, Event>() {
                    @Override
                    public void apply(EventKey eventKey, Event event) {
                        System.out.println("got event: " + event.toString());
                    }
                }).filter(new Predicate<EventKey, Event>() {
                    @Override
                    public boolean test(EventKey eventKey, Event event) {
                        boolean isType1 = event.getType().toString().equals("type1");
                        System.out.println(event + " type1: " + isType1 );
                        return isType1;
                    }
                });
    }
}