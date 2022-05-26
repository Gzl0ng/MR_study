package com.Gzl0ng.mapreduce.writable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 1.定义类实现writable接口
 * 2.重写序列化反序列化方法
 * 3.重写空参构造器
 * 4.toString方法
 */
public class FlowBean implements Writable{
    private long upFlow;//上行流量
    private long dowFlow;//下行流量
    private long sumFlow;//总流量

    //空参构造器
    public FlowBean() {
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public void setDowFlow(long dowFlow) {
        this.dowFlow = dowFlow;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public long getDowFlow() {
        return dowFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    public void setSumFlow() {
        this.sumFlow = this.upFlow + this.dowFlow;
    }

    @Override
    public void write(DataOutput out) throws IOException {

        out.writeLong(upFlow);
        out.writeLong(dowFlow);
        out.writeLong(sumFlow);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.upFlow = in.readLong();
        this.dowFlow = in.readLong();
        this.sumFlow = in.readLong();
    }

    @Override
    public String toString() {
        return upFlow +  "\t" + dowFlow + "\t" + sumFlow;
    }
}
