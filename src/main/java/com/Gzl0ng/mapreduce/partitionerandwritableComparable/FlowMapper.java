package com.Gzl0ng.mapreduce.partitionerandwritableComparable;

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


        outK.setUpFlow(Long.parseLong(words[words.length - 3]));
        outK.setDownFlow(Long.parseLong(words[words.length - 2]));
        outK.setSumFlow();
        outV.set(words[1]);

        context.write(outK,outV);
    }
}
