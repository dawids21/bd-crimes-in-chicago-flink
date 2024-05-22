package xyz.stasiak.bigdata.model;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Table(keyspace = "crime_data", name = "crime_aggregate")
public class CrimeAggregate {
    @Column(name = "month")
    private int month;
    @Column(name = "primary_description")
    private String primaryDescription;
    @Column(name = "district")
    private int district;
    @Column(name = "count")
    private long count;
    @Column(name = "count_arrest")
    private long countArrest;
    @Column(name = "count_domestic")
    private long countDomestic;
    @Column(name = "count_monitored_by_fbi")
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
