package com.ballaci.model;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Getter
@Setter
public class StoreItem implements Serializable {

    private static long TTL_IN_MILS = 3000;

    public StoreItem() {
    }

    List<OcrReadyEvent> events = new ArrayList<>();

    public StoreItem(OcrReadyEvent event) {
        events.add(event);
        this.totalMessages = event.getTotal();
        this.creationTime = Instant.now().toEpochMilli();
        this.count.incrementAndGet();
    }


    @Override
    public int hashCode() {
        return Objects.hash(events, count, totalMessages);
    }

    AtomicInteger count = new AtomicInteger(0);
    private int totalMessages;
    private long creationTime;

    public AtomicInteger getCount() {
        return count;
    }

    public boolean addEvent(OcrReadyEvent e) {
        return events.add(e);
    }

    public void setCount(AtomicInteger count) {
        this.count = count;
    }

    public boolean isComplete() {
        return this.count.get() == this.getTotalMessages();
    }

    public OcrAggregatedEvent finalise() {
        return finalise("aggregated");
    }

    public OcrAggregatedEvent finalise(String message) {
        OcrAggregatedEvent event = new OcrAggregatedEvent();
        List<String> list = new ArrayList<>();
        for (OcrReadyEvent ocrReadyEvent : events) {
            String fileRef = ocrReadyEvent.getFileRef();
            list.add(fileRef);
        }
        event.setFileRefs(list);
        event.setAggregatedMessages(this.getCount().intValue());
        event.setTotalMessages(this.getTotalMessages());
        event.setStatus(this.isComplete());
        event.setMessage(message);
        return event;
    }

    public int incrementAndGetCount() {
        return this.count.incrementAndGet();
    }

    public boolean hasExpired() {
        long now = Instant.now().toEpochMilli();
        log.warn("now: {}, creationTime: {}, ttl: {} - isExpired: {}", now, this.getCreationTime(), TTL_IN_MILS, now - this.getCreationTime() > TTL_IN_MILS);
        return now - this.getCreationTime() > TTL_IN_MILS;
    }

    @Override
    public String toString() {
        return "StoreItem{" +
                "count=" + count +
                ", totalMessages=" + totalMessages +
                ", creationTime=" + creationTime +
                '}';
    }
}
