package xyz.stasiak.bigdata.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CrimeAggregate {
    private int month;
    private String primaryDescription;
    private int district;
    private long count;
    private long countArrest;
    private long countDomestic;
    private long countMonitoredByFbi;

    public static CrimeAggregate fromCrimeFbi(CrimeFbi crime) {
        return new CrimeAggregate(
                crime.getDate().getMonth().getValue(),
                crime.getPrimaryDescription(),
                crime.getDistrict(), 1,
                crime.isArrest() ? 1 : 0,
                crime.isDomestic() ? 1 : 0,
                crime.isMonitoredByFbi() ? 1 : 0
        );
    }
}
