package com.example.bigdata.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CrimeAnomalyAggregate {
    private int district;
    private long count;
    private long countMonitoredByFbi;

    public static CrimeAnomalyAggregate fromCrimeFbi(CrimeFbi crime) {
        return new CrimeAnomalyAggregate(
                crime.getDistrict(), 1,
                crime.isMonitoredByFbi() ? 1 : 0
        );
    }
}
