package com.ballaci.processors;


import com.ballaci.model.OcrReadyEvent;
import com.ballaci.model.StoreItem;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;

public class OcrEventTransformer implements Transformer<String, OcrReadyEvent, OcrReadyEvent> {
    private ProcessorContext context;
    private KeyValueStore<String, StoreItem> kvStore;

    private String stateStoreName;

    public OcrEventTransformer(String stateStoreName) {
        this.stateStoreName = stateStoreName;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        // keep the processor context locally because we need it in punctuate() and commit()
        this.context = context;

        // retrieve the key-value store named "Counts"
        kvStore = (KeyValueStore) context.getStateStore("ocr-store");

        // schedule a punctuate() method every second based on event-time
        this.context.schedule(Duration.ofSeconds(1), PunctuationType.WALL_CLOCK_TIME, (timestamp) -> {
            KeyValueIterator<String, StoreItem> it = this.kvStore.all();
            while (it.hasNext()) {
                KeyValue<String, StoreItem> entry = it.next();
                if(entry.value.isComplete() || entry.value.hasExpired()){
                    kvStore.delete(entry.key);
                    context.forward(entry.key, entry.value.finalise());
                }
            }
            it.close();
            // commit the current processing progress
            context.commit();
        });
    }

    @Override
    public OcrReadyEvent transform(String key, OcrReadyEvent event) {
        if (event.getPart() == event.getTotal()) {
            return  new StoreItem(event).finalise();
        } else {
            StoreItem state = this.kvStore.get(key);
            if (state == null) {
                this.kvStore.put(key, new StoreItem(event));
            } else {
                state.addEvent(event);
                if(state.isComplete()){
                    return new StoreItem(event).finalise();
                }
            }
        }
        return null;
    }



    @Override
    public void close() {
        // close any resources managed by this processor.
        // Note: Do not close any StateStores as these are managed
        // by the library
    }
};
