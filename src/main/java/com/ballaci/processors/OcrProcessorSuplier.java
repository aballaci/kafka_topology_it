package com.ballaci.processors;

import com.ballaci.model.OcrReadyEvent;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;

public class OcrProcessorSuplier implements ProcessorSupplier<String, OcrReadyEvent> {
    @Override
    public Processor<String, OcrReadyEvent> get() {
        return new OcrEventProcessor();
    }
}
