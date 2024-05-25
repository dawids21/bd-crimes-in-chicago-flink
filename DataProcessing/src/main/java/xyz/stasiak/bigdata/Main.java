package xyz.stasiak.bigdata;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import xyz.stasiak.bigdata.connectors.Connectors;
import xyz.stasiak.bigdata.functions.ControlFunction;
import xyz.stasiak.bigdata.functions.CrimeAnomalyProcessFunction;
import xyz.stasiak.bigdata.functions.CrimeFbiEnrichmentFunction;
import xyz.stasiak.bigdata.model.*;
import xyz.stasiak.bigdata.windows.EveryEventTimeTrigger;
import xyz.stasiak.bigdata.windows.MonthTumblingEventTimeWindows;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneOffset;

public class Main {

    public static MapStateDescriptor<String, IucrCode> iucrCodeStateDescriptor = new MapStateDescriptor<>(
            "iucrCodeState",
            BasicTypeInfo.STRING_TYPE_INFO,
            TypeInformation.of(IucrCode.class)
    );

    public static void main(String[] args) throws Exception {

        ParameterTool propertiesFromFile = ParameterTool.fromPropertiesFile(Main.class.getResourceAsStream("/flink.properties"));
        ParameterTool propertiesFromArgs = ParameterTool.fromArgs(args);
        ParameterTool properties = propertiesFromFile.mergeWith(propertiesFromArgs);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        DataStream<Crime> crimesSource = env.fromSource(Connectors.getCrimesSource(properties), WatermarkStrategy.noWatermarks(), "Crimes Source");
        DataStream<Crime> crimesSource = env.fromElements(
                new Crime(1, LocalDateTime.of(2023, Month.JANUARY, 1, 8, 0), "0110", true, false, 1, 1, 1.0, 1.0),
                new Crime(2, LocalDateTime.of(2023, Month.JANUARY, 1, 8, 0), "0141", true, true, 1, 1, 1.0, 1.0),
                new Crime(3, LocalDateTime.of(2023, Month.JANUARY, 1, 8, 0), "0141", true, false, 1, 1, 1.0, 1.0),
                new Crime(4, LocalDateTime.of(2023, Month.JANUARY, 1, 8, 0), "0110", false, false, 1, 1, 1.0, 1.0),
                new Crime(5, LocalDateTime.of(2023, Month.FEBRUARY, 1, 8, 0), "0110", true, false, 1, 1, 1.0, 1.0),
                new Crime(6, LocalDateTime.of(2023, Month.MARCH, 1, 8, 0), "0141", false, true, 1, 1, 1.0, 1.0),
                new Crime(7, LocalDateTime.of(2023, Month.MARCH, 2, 0, 0), "0141", false, true, 1, 1, 1.0, 1.0),
                new Crime(8, LocalDateTime.of(2023, Month.APRIL, 9, 0, 0), "0141", false, false, 1, 1, 1.0, 1.0),
                new Crime(9, LocalDateTime.of(2023, Month.APRIL, 9, 0, 0), "0141", false, false, 1, 1, 1.0, 1.0),
                new Crime(10, LocalDateTime.of(2023, Month.APRIL, 9, 0, 0), "0141", false, false, 1, 1, 1.0, 1.0),
                new Crime(11, LocalDateTime.of(2023, Month.APRIL, 10, 0, 0), "0110", false, false, 1, 1, 1.0, 1.0),
                new Crime(12, LocalDateTime.of(2023, Month.APRIL, 13, 0, 0), "0110", false, false, 1, 1, 1.0, 1.0),
                new Crime(13, LocalDateTime.of(2023, Month.APRIL, 17, 12, 0), "0141", false, false, 1, 1, 1.0, 1.0),
                new Crime(14, LocalDateTime.of(2023, Month.APRIL, 16, 13, 0), "0141", false, false, 1, 1, 1.0, 1.0),
                new Crime(15, LocalDateTime.of(2023, Month.APRIL, 18, 0, 0), "0110", false, false, 1, 1, 1.0, 1.0)
        );
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
                .window(new MonthTumblingEventTimeWindows())
                .trigger(new EveryEventTimeTrigger()) // not commented = delay = A, commented = delay = C
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

        DataStream<String> anomalyOutput = crimeFbiStream
                .map(CrimeAnomalyAggregate::fromCrimeFbi)
                .keyBy(CrimeAnomalyAggregate::getDistrict)
                .window(SlidingEventTimeWindows.of(Time.days(7), Time.days(1)))
                .reduce((crime1, crime2) -> {
                    CrimeAnomalyAggregate crimeAnomalyAggregate = new CrimeAnomalyAggregate();
                    crimeAnomalyAggregate.setDistrict(crime1.getDistrict());
                    crimeAnomalyAggregate.setCount(crime1.getCount() + crime2.getCount());
                    crimeAnomalyAggregate.setCountMonitoredByFbi(crime1.getCountMonitoredByFbi() + crime2.getCountMonitoredByFbi());
                    return crimeAnomalyAggregate;
                }, new CrimeAnomalyProcessFunction())
                .process(new ProcessFunction<CrimeAnomalyResult, CrimeAnomalyResult>() {
                    @Override
                    public void processElement(CrimeAnomalyResult value, ProcessFunction<CrimeAnomalyResult, CrimeAnomalyResult>.Context ctx, Collector<CrimeAnomalyResult> out) {
                        System.out.println(value);
                        out.collect(value);
                    }
                })
                .filter(crimeAnomalyResult -> crimeAnomalyResult.getPercentageMonitoredByFbi() > 0.4)
                .map(CrimeAnomalyResult::toString);

        Connectors.getCassandraAggSink(aggOutput, properties);
        anomalyOutput.sinkTo(Connectors.getAnomalySink(properties));

        env.execute("FlinkCrimesChicago");
    }
}
