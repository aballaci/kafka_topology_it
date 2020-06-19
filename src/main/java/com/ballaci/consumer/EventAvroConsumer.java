package com.ballaci.consumer;

import com.ballaci.model.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.stereotype.Service;

@Service
public class EventAvroConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventAvroConsumer.class);

    @StreamListener(Processor.INPUT)
    public void consumeEvent(Event event) {
        LOGGER.info("Let's process event details: {}", event);
    }

}
