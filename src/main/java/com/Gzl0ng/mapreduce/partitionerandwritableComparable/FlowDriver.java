package com.Gzl0ng.mapreduce.partitionerandwritableComparable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2.jar
        job.setJarByClass(FlowDriver.class);

        //3.map和reduce的关联
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        //4.map输出
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        //5.reduce输出
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //FileOutputFormat方式和分片大小
//        job.setInputFormatClass();
//        CombineTextInputFormat.setMaxInputSplitSize(job,113324L);
        //分区类和分区数量
        job.setPartitionerClass(ProvincePartitioner2.class);
        job.setNumReduceTasks(5);

        //6.输入输出路径
        FileInputFormat.setInputPaths(job, new Path("E:\\atguigu\\hadoop\\test\\input"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\atguigu\\hadoop\\test\\output\\output3"));

        //7.提交
        boolean result = job.waitForCompletion(true);
        System.out.println(result ? 0 : 1);
    }
}
