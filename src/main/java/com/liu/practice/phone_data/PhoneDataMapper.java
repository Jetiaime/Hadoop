package com.liu.practice.phone_data;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PhoneDataMapper extends Mapper<LongWritable, Text, Text, PhoneDataBean> {

    private final Text phoneNum = new Text();
    private final PhoneDataBean flow = new PhoneDataBean();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, PhoneDataBean>.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] phoneData = line.split("\t");
        flow.setUpFlow(Long.parseLong(phoneData[phoneData.length - 3]));
        flow.setDownFlow(Long.parseLong(phoneData[phoneData.length - 2]));
        phoneNum.set(phoneData[1]);
        context.write(phoneNum, flow);
    }
}
