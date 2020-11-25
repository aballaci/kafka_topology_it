package com.ballaci.model;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.Set;

public class StoreItem {

    private static long TTL_IN_MILS = 5_000L;

    public StoreItem(OcrReadyEvent event) {
        this.addEvent(event);
        this.totalMessages = event.getTotal();
        this.creationTime = System.currentTimeMillis();
    }

    private Set<OcrReadyEvent> events = new HashSet<>();

    private int totalMessages;

    private long creationTime;


    public boolean addEvent(OcrReadyEvent event){
        if(event.getTotal() != this.getTotalMessages()){
            System.out.println("This can not be its an error");
        }
        return events.add(event);
    }

    public boolean isComplete(){
        return events.size() == this.getTotalMessages();
    }

    public int getTotalMessages() {
        return totalMessages;
    }

    public OcrReadyEvent finalise(){
        OcrReadyEvent event = new OcrReadyEvent();
        event.setStatus(this.isComplete());
        return event;
    }

    public boolean hasExpired(){
        return System.currentTimeMillis() - this.creationTime > TTL_IN_MILS;
    }
}
