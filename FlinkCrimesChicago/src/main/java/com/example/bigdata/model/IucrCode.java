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

    public static IucrCode fromString(String line) {
        String[] fields = line.split(",");
        String code = fields[0];
        if (code.length() == 3) {
            code = "0" + fields[0];
        }
        return new IucrCode(code, fields[1], fields[2], "I".equals(fields[3]));
    }
}
