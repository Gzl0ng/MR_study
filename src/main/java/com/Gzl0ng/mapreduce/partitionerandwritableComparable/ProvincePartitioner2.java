package com.Gzl0ng.mapreduce.partitionerandwritableComparable;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincePartitioner2 extends Partitioner<FlowBean, Text> {
    @Override
    public int getPartition(FlowBean flowBean, Text text, int numPartitions) {
        String phone = text.toString();
        String s = phone.substring(0,3);
        if ("136".equals(s)){
            return 0;
        }else if ("137".equals(s)){
            return 1;
        }else if ("138".equals(s)){
            return 2;
        }else if ("139".equals(s)){
            return 3;
        }else {
            return 4;
        }
    }
}
