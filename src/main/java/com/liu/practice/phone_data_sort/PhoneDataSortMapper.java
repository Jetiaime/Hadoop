package com.liu.practice.phone_data_sort;

import com.liu.practice.phone_data.PhoneDataBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PhoneDataSortMapper extends Mapper<LongWritable, Text, PhoneDataBean, Text> {

    private final PhoneDataBean flow = new PhoneDataBean();
    private final Text phoneNum = new Text();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, PhoneDataBean, Text>.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] flowData = line.split("\t");
        flow.setUpFlow(Long.parseLong(flowData[1]));
        flow.setDownFlow(Long.parseLong(flowData[2]));
        phoneNum.set(flowData[0]);
        context.write(flow, phoneNum);
    }
}
