package com.Gzl0ng.mapreduce.combineTextInputformat;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //2.jar
        job.setJarByClass(WordCountDriver.class);
        //3.map和reduce关联
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        //4.map阶段输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //5.reduce阶段输出
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //如果不设置inputFormat，默认为FileInputFormat.class
        job.setInputFormatClass(CombineTextInputFormat.class);

        //虚拟存储切片最大值设置4m
//        CombineTextInputFormat.setMaxInputSplitSize(job,4194304);
        CombineTextInputFormat.setMaxInputSplitSize(job,20971520);

        //6.输入输出路径
        FileInputFormat.setInputPaths(job, new Path("E:\\atguigu\\hadoop\\inputword"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\atguigu\\hadoop\\combine3"));
        //7.提交
        boolean result = job.waitForCompletion(true);
        System.out.println(result ? 0 : 1);
    }
}
