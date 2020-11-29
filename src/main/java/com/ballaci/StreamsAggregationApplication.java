package com.ballaci;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.schema.client.EnableSchemaRegistryClient;

@SpringBootApplication
@EnableSchemaRegistryClient
public class StreamsAggregationApplication {

    public static void main(String[] args) {
        SpringApplication.run(StreamsAggregationApplication.class, args);
    }

}
