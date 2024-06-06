package com.example.bigdata.windows;

import lombok.AllArgsConstructor;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.Collections;

@AllArgsConstructor
public class MonthTumblingEventTimeWindows extends WindowAssigner<Object, TimeWindow> {

    private final String delay;

    @Override
    public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssigner.WindowAssignerContext context) {
        LocalDateTime start = Instant.ofEpochMilli(timestamp)
                .atOffset(ZoneOffset.UTC)
                .toLocalDate()
                .withDayOfMonth(1)
                .atStartOfDay();
        long startMilli = start.toEpochSecond(ZoneOffset.UTC) * 1000;
        LocalDateTime end = start.plusMonths(1);
        long endMilli = end.toEpochSecond(ZoneOffset.UTC) * 1000;
        return Collections.singletonList(new TimeWindow(startMilli, endMilli));
    }

    @Override
    public Trigger<Object, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
        return delay.equals("A") ? EveryEventTimeTrigger.create() : EventTimeTrigger.create();
    }

    @Override
    public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
        return new TimeWindow.Serializer();
    }

    @Override
    public boolean isEventTime() {
        return true;
    }

    public String toString() {
        return "MonthTumblingEventTimeWindows()";
    }
}
