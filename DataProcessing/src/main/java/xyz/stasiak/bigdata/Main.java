package xyz.stasiak.bigdata;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import xyz.stasiak.bigdata.connectors.Connectors;
import xyz.stasiak.bigdata.functions.ControlFunction;
import xyz.stasiak.bigdata.functions.CrimeAnomalyProcessFunction;
import xyz.stasiak.bigdata.functions.CrimeFbiEnrichmentFunction;
import xyz.stasiak.bigdata.model.*;
import xyz.stasiak.bigdata.windows.MonthTumblingEventTimeWindows;

import java.time.Duration;
import java.time.ZoneOffset;

public class Main {

    public static MapStateDescriptor<String, IucrCode> iucrCodeStateDescriptor = new MapStateDescriptor<>(
            "iucrCodeState",
            BasicTypeInfo.STRING_TYPE_INFO,
            TypeInformation.of(IucrCode.class)
    );

    public static void main(String[] args) throws Exception {

        ParameterTool properties = ParameterTool.fromArgs(args);
        System.out.println("Properties");
        properties.toMap().forEach((k, v) -> System.out.println(k + ": " + v));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000));
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        if (properties.has(Parameters.FLINK_CHECKPOINT_DIR)) {
            env.getCheckpointConfig().setCheckpointStorage(properties.get(Parameters.FLINK_CHECKPOINT_DIR));
        }

        DataStream<Crime> crimesSource = env.fromSource(Connectors.getCrimesSource(properties), WatermarkStrategy.noWatermarks(), "Crimes Source");
        DataStream<IucrCode> iucrSource = env.fromSource(Connectors.getIucrSource(properties), WatermarkStrategy.noWatermarks(), "Iucr Source");

        BroadcastStream<IucrCode> iucrCodeBroadcastStream = iucrSource.broadcast(iucrCodeStateDescriptor);

        DataStream<CrimeFbi> crimeFbiStream = crimesSource
                .connect(iucrSource)
                .keyBy(Crime::getIucrCode, IucrCode::getCode)
                .flatMap(new ControlFunction())
                .connect(iucrCodeBroadcastStream)
                .process(new CrimeFbiEnrichmentFunction(iucrCodeStateDescriptor))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<CrimeFbi>forBoundedOutOfOrderness(Duration.ofDays(1))
                                .withTimestampAssigner((event, timestamp) -> event.getDate().toEpochSecond(ZoneOffset.UTC) * 1000));

        DataStream<CrimeAggregate> aggOutput = crimeFbiStream
                .map(CrimeAggregate::fromCrimeFbi)
                .keyBy(crimeFbi -> Tuple2.of(crimeFbi.getPrimaryDescription(), crimeFbi.getDistrict()),
                        TupleTypeInfo.getBasicAndBasicValueTupleTypeInfo(String.class, Integer.class))
                .window(new MonthTumblingEventTimeWindows(properties.get(Parameters.FLINK_DELAY, "C")))
                .reduce((crime1, crime2) -> {
                    CrimeAggregate crimeAggregate = new CrimeAggregate();
                    crimeAggregate.setMonth(crime1.getMonth());
                    crimeAggregate.setPrimaryDescription(crime1.getPrimaryDescription());
                    crimeAggregate.setDistrict(crime1.getDistrict());
                    crimeAggregate.setCount(crime1.getCount() + crime2.getCount());
                    crimeAggregate.setCountArrest(crime1.getCountArrest() + crime2.getCountArrest());
                    crimeAggregate.setCountDomestic(crime1.getCountDomestic() + crime2.getCountDomestic());
                    crimeAggregate.setCountMonitoredByFbi(crime1.getCountMonitoredByFbi() + crime2.getCountMonitoredByFbi());
                    return crimeAggregate;
                });

        double anomalyThreshold = properties.getDouble(Parameters.FLINK_ANOMALY_THRESHOLD, 60);
        DataStream<String> anomalyOutput = crimeFbiStream
                .map(CrimeAnomalyAggregate::fromCrimeFbi)
                .keyBy(CrimeAnomalyAggregate::getDistrict)
                .window(SlidingEventTimeWindows.of(Time.days(properties.getInt(Parameters.FLINK_ANOMALY_PERIOD, 30)), Time.days(1)))
                .reduce((crime1, crime2) -> {
                    CrimeAnomalyAggregate crimeAnomalyAggregate = new CrimeAnomalyAggregate();
                    crimeAnomalyAggregate.setDistrict(crime1.getDistrict());
                    crimeAnomalyAggregate.setCount(crime1.getCount() + crime2.getCount());
                    crimeAnomalyAggregate.setCountMonitoredByFbi(crime1.getCountMonitoredByFbi() + crime2.getCountMonitoredByFbi());
                    return crimeAnomalyAggregate;
                }, new CrimeAnomalyProcessFunction())
                .filter(crimeAnomalyResult -> crimeAnomalyResult.getPercentageMonitoredByFbi() > anomalyThreshold)
                .map(CrimeAnomalyResult::toString);

        Connectors.getCassandraAggSink(aggOutput, properties);
        anomalyOutput.sinkTo(Connectors.getAnomalySink(properties));

        env.execute("FlinkCrimesChicago");
    }
}
