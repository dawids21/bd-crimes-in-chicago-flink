package com.example.bigdata.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Crime implements Serializable {
    private long id;
    private LocalDateTime date;
    private String iucrCode;
    private boolean arrest;
    private boolean domestic;
    private int district;
}
