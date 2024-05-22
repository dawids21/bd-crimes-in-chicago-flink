package xyz.stasiak.bigdata.functions;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;
import xyz.stasiak.bigdata.model.Crime;
import xyz.stasiak.bigdata.model.IucrCode;

public class ControlFunction extends RichCoFlatMapFunction<Crime, IucrCode, Crime> {

    private ValueState<Boolean> blocked;
    private ValueState<Boolean> bufferNotEmpty;
    private ListState<Crime> bufferedCrimes;

    @Override
    public void open(Configuration parameters) {
        blocked = getRuntimeContext()
                .getState(new ValueStateDescriptor<>("blocked", Boolean.class));
        bufferNotEmpty = getRuntimeContext()
                .getState(new ValueStateDescriptor<>("bufferNotEmpty", Boolean.class));
        bufferedCrimes = getRuntimeContext()
                .getListState(new ListStateDescriptor<>("bufferedCrimes", Crime.class));
    }

    @Override
    public void flatMap1(Crime value, Collector<Crime> out) throws Exception {
        if (blocked.value() == null || blocked.value()) {
            bufferedCrimes.add(value);
            bufferNotEmpty.update(true);
        } else {
            if (bufferNotEmpty.value() != null && bufferNotEmpty.value()) {
                for (Crime crime : bufferedCrimes.get()) {
                    out.collect(crime);
                }
                bufferedCrimes.clear();
                bufferNotEmpty.update(false);
            }
            out.collect(value);
        }
    }

    @Override
    public void flatMap2(IucrCode value, Collector<Crime> out) throws Exception {
        blocked.update(false);
        if (bufferNotEmpty.value() != null && bufferNotEmpty.value()) {
            for (Crime crime : bufferedCrimes.get()) {
                out.collect(crime);
            }
            bufferedCrimes.clear();
            bufferNotEmpty.update(false);
        }
    }
}
