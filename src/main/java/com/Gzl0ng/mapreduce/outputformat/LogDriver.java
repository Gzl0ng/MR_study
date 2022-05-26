package com.Gzl0ng.mapreduce.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class LogDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2.jar
        job.setJarByClass(LogDriver.class);

        //3.map和reducer关联
        job.setMapperClass(LogMapper.class);
        job.setReducerClass(LogReducer.class);

        //4.map输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        //5.reduce输出
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //设置Fileinputformat方式和分片大小
//        job.setInputFormatClass();
//        CombineTextInputFormat.setMaxInputSplitSize(job,11312L);
        //设置分区类和分区数
//        job.setPartitionerClass();
//        job.setNumReduceTasks(5);
        //设置自定义的outPutFormat
        job.setOutputFormatClass(LogOutPutFormat.class);

        //6.输入输出路径
        //虽然我们自定义了outputFormat，但是因为我们的outputformat继承自fileoutputformat
        //而fileoutputformat要输出一个_SUCCESS文件，所以在这还得指定一个输出目录
        FileInputFormat.setInputPaths(job,new Path("E:\\atguigu\\hadoop\\test\\input"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\atguigu\\hadoop\\test\\output\\output8"));

        //7.提交
        boolean result = job.waitForCompletion(true);
        System.out.println(result ? 0 : 1);
    }
}
