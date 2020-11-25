package com.ballaci.processors;

import com.ballaci.model.OcrReadyEvent;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;

public class OcrTransformerSuplier implements TransformerSupplier<String, OcrReadyEvent, OcrReadyEvent> {


    private String stateStoreName;

    public OcrTransformerSuplier(String stateStoreName) {
        this.stateStoreName = stateStoreName;
    }

    @Override
    public Transformer<String, OcrReadyEvent, OcrReadyEvent> get() {
        return new OcrEventTransformer(stateStoreName);
    }
}
