package com.ballaci.consumer;

import com.ballaci.model.Event;
import com.ballaci.model.OcrReadyEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

//@Component
@EnableBinding(Sink.class)
public class OcrEventConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(OcrEventConsumer.class);

    @StreamListener(Processor.INPUT)
    public void consumeEvent(OcrReadyEvent event) {

        LOGGER.info("Got event: {}", event);
    }

}
