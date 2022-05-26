package com.Gzl0ng.mapreduce.yasuo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.job
        Configuration conf = new Configuration();

//        conf.setBoolean("mapreduce.map.output.compress","true");

//        conf.setClass("mapreduce.map.output.compress.codec", BZip2Codec, CompressionCodec);

        Job job = Job.getInstance(conf);

        //2.jar
        job.setJarByClass(WordCountDriver.class);

        //3.map和reduce关联
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //4.map输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //5.reduce输出
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //6.输入输出路径
        FileInputFormat.setInputPaths(job, new Path(""));
        FileOutputFormat.setOutputPath(job, new Path(""));

        //7.提交
        boolean b = job.waitForCompletion(true);
        System.out.println(b ? 0 : 1);
    }
}
