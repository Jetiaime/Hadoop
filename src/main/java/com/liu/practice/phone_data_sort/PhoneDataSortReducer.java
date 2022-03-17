package com.liu.practice.phone_data_sort;

import com.liu.practice.phone_data.PhoneDataBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;

public class PhoneDataSortReducer extends Reducer<PhoneDataBean, Text, Text, PhoneDataBean> {
    @Override
    protected void reduce(PhoneDataBean key, Iterable<Text> values, Reducer<PhoneDataBean, Text, Text, PhoneDataBean>.Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(value, key);
        }
    }
}
