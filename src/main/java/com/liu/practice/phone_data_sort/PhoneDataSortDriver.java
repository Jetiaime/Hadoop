package com.liu.practice.phone_data_sort;

import com.liu.practice.phone_data.PhoneDataBean;
import com.liu.practice.phone_data.PhoneDataPartitioner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class PhoneDataSortDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(PhoneDataSortDriver.class);
        job.setMapperClass(PhoneDataSortMapper.class);
        job.setReducerClass(PhoneDataSortReducer.class);

        job.setMapOutputKeyClass(PhoneDataBean.class);
        job.setMapOutputValueClass(Text.class);

        job.setPartitionerClass(PhoneDataSortPartitioner.class);
        job.setNumReduceTasks(5);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PhoneDataBean.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean res = job.waitForCompletion(true);

        System.exit(res ? 0 : 1);
    }
}
