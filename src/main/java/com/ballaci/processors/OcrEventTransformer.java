package com.ballaci.processors;


import com.ballaci.CustomSerdes;
import com.ballaci.model.OcrAggregatedEvent;
import com.ballaci.model.OcrReadyEvent;
import com.ballaci.model.StoreItem;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
@Slf4j
public class OcrEventTransformer implements Transformer<String, OcrReadyEvent, KeyValue<String, OcrAggregatedEvent>> {

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
        kvStore = (KeyValueStore) context.getStateStore(stateStoreName);

        // schedule a punctuate() method every second based on event-time
        this.context.schedule(Duration.ofMillis(1000), PunctuationType.WALL_CLOCK_TIME, (timestamp) -> {
            try {
                log.trace("Punctuating....");
                KeyValueIterator<String, StoreItem> it = this.kvStore.all();
                while (it.hasNext()) {
                    KeyValue<String, StoreItem> entry = it.next();
                    log.trace("entry key {}", entry.key);
                    if (entry.value.hasExpired()) {
                        log.trace("XXX Invalidating message key: {} val {} XXX", entry.key, entry.value);
                        String key = entry.key;
                        OcrAggregatedEvent event  = entry.value.finalise("expired");
                        context.forward(key, event );
                        kvStore.delete(entry.key);
                    }
                }
                it.close();
                // commit the current processing progress
                context.commit();
            } catch (Exception e) {
                log.error("error in punctuator", e.getMessage());
            }
        });

    }

    @Override
    public KeyValue<String, OcrAggregatedEvent> transform(String key, OcrReadyEvent event) {
        try {
            log.trace("transforming {} msg {} of {}", key, event.getPart(), event.getTotal());
            StoreItem storeItem = this.kvStore.get(key);
            if (storeItem == null) {
                storeItem = new StoreItem(event);
            } else {
                storeItem.incrementAndGetCount();
                storeItem.addEvent(event);
            }
            kvStore.put(key, storeItem);
            if (storeItem.isComplete()) {
                kvStore.delete(key);
                return KeyValue.pair(key, storeItem.finalise());
            }
        } catch (Exception e) {
            log.error("error in transformer");
            e.printStackTrace();
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
