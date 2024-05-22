package xyz.stasiak.bigdata.model;

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
    private int communityArea;
    private double latitude;
    private double longitude;
    private String primaryDescription;
    private boolean monitoredByFbi;

    public static CrimeFbi fromCrime(Crime crime) {
        return new CrimeFbi(crime.getId(), crime.getDate(), crime.getIucrCode(),
                crime.isArrest(), crime.isDomestic(), crime.getDistrict(),
                crime.getCommunityArea(), crime.getLatitude(), crime.getLongitude(),
                "", false);
    }

    public static CrimeFbi fromCrime(Crime crime, IucrCode iucrCode) {
        return new CrimeFbi(crime.getId(), crime.getDate(), crime.getIucrCode(),
                crime.isArrest(), crime.isDomestic(), crime.getDistrict(),
                crime.getCommunityArea(), crime.getLatitude(), crime.getLongitude(),
                iucrCode.getPrimaryDescription(), iucrCode.isMonitoredByFbi());
    }
}