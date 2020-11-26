package com.ballaci.processors;


import com.ballaci.consumer.CustomAggregator;
import com.ballaci.model.OcrReadyEvent;
import com.ballaci.model.StoreItem;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class OcrEventTransformer implements Transformer<String, OcrReadyEvent, OcrReadyEvent> {

    private static final Logger LOGGER = LoggerFactory.getLogger(OcrEventTransformer.class);
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
        this.context.schedule(Duration.ofSeconds(1), PunctuationType.WALL_CLOCK_TIME, (timestamp) -> {
            try {
                KeyValueIterator<String, StoreItem> it = this.kvStore.all();
                while (it.hasNext()) {
                    KeyValue<String, StoreItem> entry = it.next();
                    if (entry.value.isComplete() || entry.value.hasExpired()) {
//                    kvStore.delete(entry.key);
                        context.forward(entry.key, entry.value.finalise());
                    }
                }
                it.close();
                // commit the current processing progress
                context.commit();
            } catch (Exception e) {
                LOGGER.error("error in punctuator", e.getMessage());
            }
        });

    }

    @Override
    public OcrReadyEvent transform(String key, OcrReadyEvent event) {
        try {
            LOGGER.info("transforming {} msg {} of {}", key, event.getPart(), event.getTotal());
            if (event.getPart() == event.getTotal()) {
                return new StoreItem(event).finalise();
            } else {
                StoreItem storeItem = this.kvStore.get(key);
                if (storeItem == null) {
                    this.kvStore.put(key, new StoreItem(event));
                } else {
                    storeItem.addEvent(event);
                    if (storeItem.isComplete()) {
                        return storeItem.finalise();
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error("error in transformer", e.getMessage());
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
