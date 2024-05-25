package xyz.stasiak.bigdata.functions;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import xyz.stasiak.bigdata.model.Crime;
import xyz.stasiak.bigdata.model.CrimeFbi;
import xyz.stasiak.bigdata.model.IucrCode;

public class CrimeFbiEnrichmentFunction extends BroadcastProcessFunction<Crime, IucrCode, CrimeFbi> {

    private final MapStateDescriptor<String, IucrCode> iucrCodeStateDescriptor;

    public CrimeFbiEnrichmentFunction(MapStateDescriptor<String, IucrCode> iucrCodeStateDescriptor) {
        this.iucrCodeStateDescriptor = iucrCodeStateDescriptor;
    }

    @Override
    public void processElement(Crime value, BroadcastProcessFunction<Crime, IucrCode, CrimeFbi>.ReadOnlyContext ctx, Collector<CrimeFbi> out) throws Exception {
        ReadOnlyBroadcastState<String, IucrCode> state = ctx.getBroadcastState(iucrCodeStateDescriptor);
        CrimeFbi crimeFbi;
        if (state.contains(value.getIucrCode())) {
            crimeFbi = CrimeFbi.fromCrime(value, state.get(value.getIucrCode()));
        } else {
            crimeFbi = CrimeFbi.fromCrime(value);
        }
        out.collect(crimeFbi);
    }

    @Override
    public void processBroadcastElement(IucrCode value, BroadcastProcessFunction<Crime, IucrCode, CrimeFbi>.Context ctx, Collector<CrimeFbi> out) throws Exception {
        BroadcastState<String, IucrCode> state = ctx.getBroadcastState(iucrCodeStateDescriptor);
        state.put(value.getCode(), value);
    }
}
