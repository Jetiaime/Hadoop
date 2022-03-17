package com.liu.practice.phone_data_sort;

import com.liu.practice.phone_data.PhoneDataBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class PhoneDataSortPartitioner extends Partitioner<PhoneDataBean, Text> {

    @Override
    public int getPartition(PhoneDataBean phoneDataBean, Text text, int numPartitions) {
        int partitionNum = 4;
        String provinceNum = text.toString().substring(0, 3);
        switch (provinceNum) {
            case "136":
                partitionNum = 0;
                break;
            case "137":
                partitionNum = 1;
                break;
            case "138":
                partitionNum = 2;
                break;
            case "139":
                partitionNum = 3;
                break;
        }
        return partitionNum;
    }
}
