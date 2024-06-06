package com.example.bigdata.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class IucrCode {
    private String code;
    private String primaryDescription;
    private String secondaryDescription;
    private boolean monitoredByFbi;
}
