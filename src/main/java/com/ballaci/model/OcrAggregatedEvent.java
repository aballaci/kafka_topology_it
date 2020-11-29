package com.ballaci.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@ToString
@EqualsAndHashCode
public class OcrAggregatedEvent implements Serializable {


    @JsonProperty
    private List<String> fileRefs = new ArrayList<>();

    @JsonProperty
    private boolean status;

    @JsonProperty
    private int totalMessages;

    @JsonProperty
    private int aggregatedMessages;

    @JsonProperty
    private String message;



}
