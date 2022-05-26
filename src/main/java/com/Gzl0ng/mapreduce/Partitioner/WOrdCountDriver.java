package com.Gzl0ng.mapreduce.Partitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WOrdCountDriver {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        //1.job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //2.jar
        job.setJarByClass(WOrdCountDriver.class);
        //3.map和reduce关联
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCoiuntReducer.class);
        //4.map阶段输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //5.reduce阶段输出
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(2);

        //设置Inputformat方式和分片大小
//        job.setInputFormatClass(CombineTextInputFormat.class);
//        CombineTextInputFormat.setMaxInputSplitSize(job,154165);
        //6.输入输出路径
        FileInputFormat.setInputPaths(job, new Path("E:\\atguigu\\hadoop\\inputword"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\atguigu\\hadoop\\output3"));
        //7.提交
        boolean result = job.waitForCompletion(true);
        System.out.println(result ? 0 : 1);
    }
}
