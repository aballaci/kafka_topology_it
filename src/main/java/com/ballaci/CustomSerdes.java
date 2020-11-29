package com.ballaci;

import com.ballaci.model.OcrAggregatedEvent;
import com.ballaci.model.OcrReadyEvent;
import com.ballaci.model.StoreItem;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

public final class CustomSerdes {

    static public final class OcrReadyEventSerde extends Serdes.WrapperSerde<OcrReadyEvent> {
        public OcrReadyEventSerde() {
            super(new JsonSerializer<>(),
                    new JsonDeserializer<>(OcrReadyEvent.class));
        }
    }

    static public final class StoreItemSerde extends Serdes.WrapperSerde<StoreItem> {
        public StoreItemSerde() {
            super(new JsonSerializer<>(),
                    new JsonDeserializer<>(StoreItem.class));
        }
    }

    static public final class OcrAggregated extends Serdes.WrapperSerde<OcrAggregatedEvent> {
        public OcrAggregated() {
            super(new JsonSerializer<>(),
                    new JsonDeserializer<>(OcrAggregatedEvent.class));
        }
    }


    public static Serde<OcrReadyEvent> OcrReadyEvent() {
        return new CustomSerdes.OcrReadyEventSerde();
    }

    public static Serde<OcrAggregatedEvent> OcrAggregatedSerde() {
        return new CustomSerdes.OcrAggregated();
    }

    public static Serde<StoreItem> StoreItemSerde() {
        return new CustomSerdes.StoreItemSerde();
    }

}
