package com.example.bigdata.functions;

import com.example.bigdata.model.CrimeAnomalyAggregate;
import com.example.bigdata.model.CrimeAnomalyResult;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.ZoneOffset;

public class CrimeAnomalyProcessFunction extends ProcessWindowFunction<CrimeAnomalyAggregate, CrimeAnomalyResult, Integer, TimeWindow> {
    @Override
    public void process(Integer district,
                        ProcessWindowFunction<CrimeAnomalyAggregate, CrimeAnomalyResult, Integer, TimeWindow>.Context context,
                        Iterable<CrimeAnomalyAggregate> elements,
                        Collector<CrimeAnomalyResult> out) {
        elements.forEach(anomaly -> out.collect(
                new CrimeAnomalyResult(
                        Instant.ofEpochMilli(context.window().getStart()).atOffset(ZoneOffset.UTC).toLocalDateTime(),
                        Instant.ofEpochMilli(context.window().getEnd()).atOffset(ZoneOffset.UTC).toLocalDateTime(),
                        anomaly.getDistrict(),
                        anomaly.getCount(),
                        anomaly.getCountMonitoredByFbi(),
                        anomaly.getCountMonitoredByFbi() / (double) anomaly.getCount() * 100.0
                )
        ));
    }
}
