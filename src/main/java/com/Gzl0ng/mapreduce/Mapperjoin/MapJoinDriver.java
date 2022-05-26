package com.Gzl0ng.mapreduce.Mapperjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class MapJoinDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {

        //1.job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2.jar
        job.setJarByClass(MapJoinDriver.class);

        //3.map关联
        job.setMapperClass(MapJoinMapper.class);

        //4.map输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        //5.reduce输出
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //加载缓存
        job.addCacheFile(new URI("file:///E:/atguigu/hadoop/test/input/input1/cache/pd.txt"));
        job.setNumReduceTasks(0);

        //6.输入输出路径
        FileInputFormat.setInputPaths(job, new Path("E:\\atguigu\\hadoop\\test\\input\\input1\\input"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\atguigu\\hadoop\\test\\output4"));

        //7.提交
        boolean b = job.waitForCompletion(true);
        System.out.println(b ? 0 : 1);
    }
}
