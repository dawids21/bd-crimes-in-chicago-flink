package com.example.bigdata.functions;

import com.example.bigdata.model.Crime;
import com.example.bigdata.model.CrimeFbi;
import com.example.bigdata.model.IucrCode;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CrimeFbiEnrichmentFunction extends RichMapFunction<Crime, CrimeFbi> {

    private final String iucrCodeFile;
    private Map<String, IucrCode> iucrCodeMap;

    public CrimeFbiEnrichmentFunction(String iucrCodeFile) {
        this.iucrCodeFile = iucrCodeFile;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        File file = getRuntimeContext().getDistributedCache().getFile(iucrCodeFile);

        try (Stream<String> lines = Files.lines(file.toPath(), StandardCharsets.UTF_8)) {
            iucrCodeMap = lines.filter(s -> !s.startsWith("IUCR"))
                    .map(IucrCode::fromString)
                    .collect(Collectors.toMap(IucrCode::getCode, iucrCode -> iucrCode));
        }
    }

    @Override
    public CrimeFbi map(Crime value) {
        CrimeFbi crimeFbi;
        if (iucrCodeMap.containsKey(value.getIucrCode())) {
            crimeFbi = CrimeFbi.fromCrime(value, iucrCodeMap.get(value.getIucrCode()));
        } else {
            crimeFbi = CrimeFbi.fromCrime(value);
        }
        return crimeFbi;
    }
}
