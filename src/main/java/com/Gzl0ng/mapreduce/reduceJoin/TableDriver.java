package com.Gzl0ng.mapreduce.reduceJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class TableDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2.jar
        job.setJarByClass(TableBean.class);

        //3.map和reduce关联
        job.setMapperClass(TableMapper.class);
        job.setReducerClass(TableReducer.class);

        //4.map输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TableBean.class);

        //5.reduce1输出
        job.setOutputKeyClass(TableBean.class);
        job.setOutputValueClass(NullWritable.class);

        //6.输入输出路径
        FileInputFormat.setInputPaths(job, new Path("E:\\atguigu\\hadoop\\test\\input\\input1"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\atguigu\\hadoop\\test\\output\\output9"));

        //7.提交
        boolean result = job.waitForCompletion(true);
        System.out.println(result ? 0 : 1);
    }
}
