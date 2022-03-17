package com.liu.practice.phone_data;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PhoneDataReducer extends Reducer<Text, PhoneDataBean, Text, PhoneDataBean> {

    private final PhoneDataBean flow = new PhoneDataBean();

    @Override
    protected void reduce(Text phoneNum, Iterable<PhoneDataBean> values, Reducer<Text, PhoneDataBean, Text, PhoneDataBean>.Context context) throws IOException, InterruptedException {
        long totalUpFlow = 0;
        long totalDownFlow = 0;
        for (PhoneDataBean value : values) {
            totalUpFlow += value.getUpFlow();
            totalDownFlow += value.getDownFlow();
        }
        flow.setUpFlow(totalUpFlow);
        flow.setDownFlow(totalDownFlow);
        context.write(phoneNum, flow);
    }
}
