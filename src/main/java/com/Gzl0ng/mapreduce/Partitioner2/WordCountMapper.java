package com.Gzl0ng.mapreduce.Partitioner2;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable,Text,Text,FlowBean> {
    private Text outK = new Text();
    private FlowBean outV = new FlowBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split("\t");

        String phone = words[1];
        String up = words[words.length - 3];
        String down = words[words.length - 2];

        outK.set(phone);
        outV.setUpFlow(Long.parseLong(up));
        outV.setDownFlow(Long.parseLong(down));
        outV.setSumFlow();

        context.write(outK,outV);
    }
}
