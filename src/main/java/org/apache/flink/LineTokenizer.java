package org.apache.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class LineTokenizer implements FlatMapFunction<String, String> {

    @Override
    public void flatMap(String value, Collector<String> out) throws Exception {
        // normalize and split the line into words
        String[] tokens = value.toLowerCase().split("\\s+|,");
        // emit the pairs
        for (String token : tokens) {
            //System.out.println("TOKEN " + token);
            if (token.length() > 0) {
                out.collect(token);
            }
        }
    }
}
