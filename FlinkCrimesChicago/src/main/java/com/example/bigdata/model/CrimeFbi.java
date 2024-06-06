package com.example.bigdata.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CrimeFbi implements Serializable {
    private long id;
    private LocalDateTime date;
    private String iucrCode;
    private boolean arrest;
    private boolean domestic;
    private int district;
    private String primaryDescription;
    private boolean monitoredByFbi;

    public static CrimeFbi fromCrime(Crime crime) {
        return new CrimeFbi(crime.getId(), crime.getDate(), crime.getIucrCode(),
                crime.isArrest(), crime.isDomestic(), crime.getDistrict(),
                "", false);
    }

    public static CrimeFbi fromCrime(Crime crime, IucrCode iucrCode) {
        return new CrimeFbi(crime.getId(), crime.getDate(), crime.getIucrCode(),
                crime.isArrest(), crime.isDomestic(), crime.getDistrict(),
                iucrCode.getPrimaryDescription(), iucrCode.isMonitoredByFbi());
    }
}