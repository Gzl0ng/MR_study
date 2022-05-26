package com.Gzl0ng.mapreduce.Partitioner2;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducer extends Reducer<Text,FlowBean,Text,FlowBean>{

    private FlowBean outV = new FlowBean();
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

        long totalUp = 0;
        long totaldowm = 0;
        for (FlowBean value : values) {
            totalUp += value.getUpFlow();
            totaldowm += value.getDownFlow();
        }
        outV.setUpFlow(totalUp);
        outV.setDownFlow(totaldowm);
        outV.setSumFlow();

        context.write(key,outV);
    }
}
