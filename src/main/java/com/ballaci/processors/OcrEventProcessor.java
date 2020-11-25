package com.ballaci.processors;


import com.ballaci.model.OcrReadyEvent;
import com.ballaci.model.StoreItem;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;

public class OcrEventProcessor implements Processor<String, OcrReadyEvent> {
    private ProcessorContext context;
    private KeyValueStore<String, StoreItem> kvStore;

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        // keep the processor context locally because we need it in punctuate() and commit()
        this.context = context;

        // retrieve the key-value store named "Counts"
        kvStore = (KeyValueStore) context.getStateStore("ocr-store");

        // schedule a punctuate() method every second based on event-time
        this.context.schedule(Duration.ofSeconds(1), PunctuationType.WALL_CLOCK_TIME, (timestamp) -> {
            KeyValueIterator<String, StoreItem> iter = this.kvStore.all();
            while (iter.hasNext()) {
                KeyValue<String, StoreItem> entry = iter.next();
                if(entry.value.isComplete() || entry.value.hasExpired()){
                    context.forward(entry.key, entry.value.finalise());
                }
            }
            iter.close();
            // commit the current processing progress
            context.commit();
        });
    }

    @Override
    public void process(String key, OcrReadyEvent event) {
        if (event.getPart() == event.getTotal()) {
            context.forward(key, new StoreItem(event).finalise());
        } else {
            StoreItem state = this.kvStore.get(key);
            if (state == null) {
                this.kvStore.put(key, new StoreItem(event));
            } else {
                state.addEvent(event);
                if(state.isComplete()){
                    context.forward(key, new StoreItem(event).finalise());
                }
            }
        }
    }

    @Override
    public void close() {
        // close any resources managed by this processor.
        // Note: Do not close any StateStores as these are managed
        // by the library
    }
};
