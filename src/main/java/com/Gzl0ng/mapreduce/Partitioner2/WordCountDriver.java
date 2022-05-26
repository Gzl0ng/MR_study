package com.Gzl0ng.mapreduce.Partitioner2;

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

        //3.关联map和reduce
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //4.map阶段输出结果
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        //5.reduce阶段输出结果
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //分区类设置，分区个数
        job.setPartitionerClass(ProvincePartitioner.class);
        job.setNumReduceTasks(5);
        //改变读取方式，设置最小分片大小
//        job.setInputFormatClass(CombineTextInputFormat.class);
//        CombineTextInputFormat.setMaxInputSplitSize(job,14131);
        //6.输入输出路径
        FileInputFormat.setInputPaths(job, new Path("E:\\atguigu\\hadoop\\inputword"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\atguigu\\hadoop\\output5"));
        //7.提交
        boolean result = job.waitForCompletion(true);
        System.out.println(result ? 0 : 1);
    }
}
