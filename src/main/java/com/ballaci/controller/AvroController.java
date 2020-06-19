package com.ballaci.controller;

import com.ballaci.producer.AvroProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class AvroController {

    @Autowired
    private AvroProducer avroProducer;

    @PostMapping("/event/{id}/{type}")
    public String producerAvroMessage(@PathVariable String id, @PathVariable String type) {
        avroProducer.produceEventDetails(id, type);
        return "Sent event details to kafka";
    }

}
