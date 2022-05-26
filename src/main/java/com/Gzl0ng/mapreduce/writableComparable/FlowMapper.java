package com.Gzl0ng.mapreduce.writableComparable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable,Text,FlowBean,Text>{

    private FlowBean outK = new FlowBean();
    private Text outV = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split("\t");

        String phone = words[1];
        String up = words[words.length - 3];
        String down = words[words.length - 2];

        outK.setUpFlow(Long.parseLong(up));
        outK.setDownFlow(Long.parseLong(down));
        outK.setSumFlow();
        outV.set(phone);

        context.write(outK,outV);
    }
}
