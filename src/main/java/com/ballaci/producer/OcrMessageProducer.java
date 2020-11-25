package com.ballaci.producer;

import com.ballaci.model.OcrReadyEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;



//@Service
public class OcrMessageProducer {

    @Autowired
    private Processor processor;

    public void publishEvent(String key, String fileRef, Boolean status) {

        OcrReadyEvent event = new OcrReadyEvent(fileRef, status);


        Message<OcrReadyEvent> message = MessageBuilder.withPayload(event)
            .setHeader(KafkaHeaders.MESSAGE_KEY, key)
            .build();

        boolean result = processor.output()
            .send(message);

        System.out.println("Sent message: "+ result);
    }

}
