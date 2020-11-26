package com.ballaci.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class StoreItem implements Serializable {

    private static long TTL_IN_MILS = 5_000L;

    public StoreItem() {
    }

    public StoreItem(OcrReadyEvent event) {
        this.addEvent(event);
        this.totalMessages = event.getTotal();
        this.creationTime = System.currentTimeMillis();
    }

    private Set<OcrReadyEvent> events = new HashSet<>();

    private int totalMessages;
    private long creationTime;


    public boolean addEvent(OcrReadyEvent event){
         return this.events.add(event);
    }

    public boolean isComplete(){
        return events.size() == this.getTotalMessages();
    }

    public int getTotalMessages() {
        return totalMessages;
    }

    public OcrReadyEvent finalise(){
        OcrReadyEvent event = new OcrReadyEvent();
        String filerefs = events.stream().map(OcrReadyEvent::getFileRef).collect(Collectors.joining(","));
        event.setFileRef(filerefs);
        event.setTotal(this.totalMessages);
        event.setStatus(this.isComplete());
        event.setPart(-1);
        return event;
    }

    public boolean hasExpired(){
        return System.currentTimeMillis() - this.creationTime > TTL_IN_MILS;
    }
}
