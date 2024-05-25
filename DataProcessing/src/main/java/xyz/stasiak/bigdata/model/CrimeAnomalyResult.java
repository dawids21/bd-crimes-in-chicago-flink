package xyz.stasiak.bigdata.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CrimeAnomalyResult {
    private LocalDateTime start;
    private LocalDateTime end;
    private int district;
    private long count;
    private long countMonitoredByFbi;
    private double percentageMonitoredByFbi;
}
