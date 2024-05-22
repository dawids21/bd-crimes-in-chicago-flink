package xyz.stasiak.bigdata;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import xyz.stasiak.bigdata.connectors.Connectors;
import xyz.stasiak.bigdata.functions.ControlFunction;
import xyz.stasiak.bigdata.functions.CrimeFbiEnrichmentFunction;
import xyz.stasiak.bigdata.model.Crime;
import xyz.stasiak.bigdata.model.CrimeAggregate;
import xyz.stasiak.bigdata.model.IucrCode;

import java.time.LocalDateTime;
import java.time.Month;

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
                new Crime(7, LocalDateTime.of(2023, Month.MARCH, 1, 8, 0), "0141", false, true, 1, 1, 1.0, 1.0)
        );
        DataStream<IucrCode> iucrSource = env.fromSource(Connectors.getIucrSource(properties), WatermarkStrategy.noWatermarks(), "Iucr Source");

        BroadcastStream<IucrCode> iucrCodeBroadcastStream = iucrSource.broadcast(iucrCodeStateDescriptor);

        DataStream<String> aggOutput = crimesSource
                .connect(iucrSource)
                .keyBy(Crime::getIucrCode, IucrCode::getCode)
                .flatMap(new ControlFunction())
                .connect(iucrCodeBroadcastStream)
                .process(new CrimeFbiEnrichmentFunction(iucrCodeStateDescriptor))
                .map(CrimeAggregate::fromCrimeFbi)
                .keyBy(crimeFbi -> Tuple3.of(crimeFbi.getMonth(), crimeFbi.getPrimaryDescription(), crimeFbi.getDistrict()),
                        TupleTypeInfo.getBasicAndBasicValueTupleTypeInfo(Integer.class, String.class, Integer.class))
                .window(SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(1))) //TODO
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
                })
                .map(CrimeAggregate::toString);

        DataStream<String> anomalyOutput = iucrSource.map(IucrCode::toString);

        aggOutput.sinkTo(Connectors.getAggSink(properties));
        anomalyOutput.sinkTo(Connectors.getAnomalySink(properties));

        env.execute("FlinkCrimesChicago");
    }
}
