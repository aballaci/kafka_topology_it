# Message aggregation through the low level Processor API 

## Problem description

We need to aggregate messages coming in any order asynchronously in a kafka topic and output an aggregated message after the transformation.

## Possible Solution

Messages are <Key, Value> pairs of binary data

our data model is as follows.

messages have keys that connect them logically.

they internally have information of the whole logical entity 
- message part e.g: 1 of 3
- totalmessages [the number of messages that make up the whole entity]

A low level Kafka Streams API is used to aggregate the messages using a persisted (log backed - resilient) State Store.

The transformer collects the parts and if all present, emits the aggregated message and cleans the State Store.

A process is scheduled at Wall Clock time Intervals to check if incomplete messages exist in the State Store and expires them
writing a Aggregation Message with an error to the downstream.

- input m<sub>1</sub>...m<sub>n</sub> -> M<sub>a</sub> (aggregated)
- input m<sub>1</sub>...m<sub>(n-1)</sub> -> M<sub>e</sub> (aggregated erroneous/incomplete after timeout)

#### The transformer

```
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
}
```

##### The SpringCloud Stream Integration in the DSL API

```
public class CustomAggregator {


    private static final String STATE_STORE_NAME = "ocr-store";

    @Bean
    public Function<KStream<String, OcrReadyEvent>, KStream<String, OcrReadyEvent>> customaggregator() {

        return input ->
                input
                  .transform((TransformerSupplier) () -> new OcrEventTransformer(STATE_STORE_NAME), Named.as("ocr-transformer"), STATE_STORE_NAME);
    }


    @Bean
    public StoreBuilder mystore() {
        return Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore(STATE_STORE_NAME), new Serdes.StringSerde(), CustomSerdes.StoreItemSerde());
    }
}
```


 ### Used technologis:
 - Springboot
 - Springcloud Kafka Streams
 - Springcloud Kafka Streams Binder
 - Embededkafka Integration Tests
 - TopologyTestDriver Unit Tests that are faster and address every aspect of the custom stream processing. 
 
 Embeded Kafka integration test and topology test for custom Streams Transformer with StateStore.
