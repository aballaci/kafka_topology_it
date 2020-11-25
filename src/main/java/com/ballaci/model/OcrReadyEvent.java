package com.ballaci.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class OcrReadyEvent {

    @JsonProperty
    private String fileRef;

    @JsonProperty
    private boolean status;

    private int parts;

    public OcrReadyEvent() {
    }

    public OcrReadyEvent(String fileRef, boolean status) {
        this.fileRef = fileRef;
        this.status = status;
    }

    public OcrReadyEvent(String fileRef, boolean status, int parts) {
        this.fileRef = fileRef;
        this.status = status;
        this.parts = parts;
    }

    public int getParts() {
        return parts;
    }

    public void setParts(int parts) {
        this.parts = parts;
    }

    public String getFileRef() {
        return fileRef;
    }

    public void setFileRef(String fileRef) {
        this.fileRef = fileRef;
    }

    public boolean isStatus() {
        return status;
    }

    public void setStatus(boolean status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "OcrReadyEvent{" +
                "fileRef='" + fileRef + '\'' +
                ", status=" + status +
                ", parts=" + parts +
                '}';
    }
}
