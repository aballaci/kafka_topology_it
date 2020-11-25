package com.ballaci.processors;

import com.ballaci.CustomSerdes;
import com.ballaci.model.OcrReadyEvent;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.binder.kafka.streams.annotations.KafkaStreamsProcessor;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Component;

import java.util.function.Function;

@Component
public class OcrReadyProcessor {

    @Bean
    public Function<KStream<String, OcrReadyEvent>, KStream<String, OcrReadyEvent>> process() {
        return input -> input
                .peek((k,v) -> System.out.println("############################## key" + k + "  val: " + v))
                .groupByKey(Grouped.with(new Serdes.StringSerde(), CustomSerdes.OcrReadyEvent()))
                .aggregate(()->new OcrReadyEvent(),(k,v, aggValue)-> new OcrReadyEvent(aggValue.getFileRef() + ", " + v.getFileRef()
                                , aggValue.isStatus() && v.isStatus(), aggValue.getParts() +1)
//                        , Materialized.with(new Serdes.StringSerde(), CustomSerdes.OcrReadyEvent())
                        )
                .toStream()
                .peek((k,v) -> System.out.println("---------Peak after aggregation key" + k + "  val: " + v));

    }


}