package com.Gzl0ng.mapreduce.etl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class webLogDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //输入输出路径需要根据自己电脑上实际的输入输出路径设置
        args = new String[]{"E:\\atguigu\\hadoop\\test\\input\\input2","E:\\atguigu\\hadoop\\test\\output5"};

        //1.job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2.jar
        job.setJarByClass(webLogDriver.class);

        //3.关联map
        job.setMapperClass(WebLogMapper.class);

        //4.map输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        //5.reduce输出
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //设置ReduceTask个数为0
        job.setNumReduceTasks(0);

        //6.输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //7.提交
        boolean b = job.waitForCompletion(true);
        System.out.println(b ? 0 : 1);
    }
}
