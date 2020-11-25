package com.ballaci.producer;

import com.ballaci.model.Event;
import com.ballaci.model.EventKey;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.jackson.JsonObjectSerializer;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;



//@Service
public class AvroProducer {

    @Autowired
    private Processor processor;

    public void produceEventDetails(String id, String type) {

        // creating employee details
        Event event = new Event();
        event.setId(id);
        event.setType(type);
        event.setTs(String.valueOf(ZonedDateTime.now(ZoneOffset.UTC).toInstant().toEpochMilli()));

        // creating partition key for kafka topic
        EventKey eventKey = new EventKey();
        eventKey.setId(id);
        eventKey.setType(type);

        Message<Event> message = MessageBuilder.withPayload(event)
            .setHeader(KafkaHeaders.MESSAGE_KEY, eventKey)
            .build();

        processor.output()
            .send(message);
    }

}
