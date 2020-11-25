package com.ballaci;

import com.ballaci.model.OcrReadyEvent;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.Assert;

import java.io.Closeable;
import java.util.Map;

public final class CustomSerdes {

    static public final class OcrReadyEventSerde extends Serdes.WrapperSerde<OcrReadyEvent> {
        public OcrReadyEventSerde() {
            super(new JsonSerializer<>(),
                    new JsonDeserializer<>(OcrReadyEvent.class));
        }
    }



    public static Serde<OcrReadyEvent> OcrReadyEvent() {
        return new CustomSerdes.OcrReadyEventSerde();
    }

}