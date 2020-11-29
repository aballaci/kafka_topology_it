package com.ballaci.processors;


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
    public void init(ProcessorContext context) {

        this.context = context;

        kvStore = (KeyValueStore) context.getStateStore(stateStoreName);

        this.context.schedule(Duration.ofMillis(1000), PunctuationType.WALL_CLOCK_TIME, (timestamp) -> invalidateExpiredMessages());

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

    private void invalidateExpiredMessages() {
        try {
            log.trace("invalidateExpiredMessages....");
            KeyValueIterator<String, StoreItem> it = this.kvStore.all();
            while (it.hasNext()) {
                KeyValue<String, StoreItem> entry = it.next();
                log.trace("entry key {}", entry.key);
                if (entry.value.hasExpired()) {
                    log.trace("Invalidating message key: {} val {}", entry.key, entry.value);
                    String key = entry.key;
                    OcrAggregatedEvent event = entry.value.finalise("expired");
                    context.forward(key, event);
                    kvStore.delete(entry.key);
                }
            }
            it.close();
            context.commit();
        } catch (Exception e) {
            log.error("error in punctuator", e.getMessage());
        }
    }


    @Override
    public void close() {
    }
};
