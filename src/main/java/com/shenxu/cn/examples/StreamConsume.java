package com.shenxu.cn.examples;

import com.shenxu.cn.client.DataLakeStreamClient;
import com.shenxu.cn.entity.DataLakeStreamData;

import java.util.List;

public class StreamConsume {
    public static void main(String[] args) throws Exception {

        DataLakeStreamClient dataLakeStreamClient = new DataLakeStreamClient("12.0.0.1", 7853);

        dataLakeStreamClient.setReadCount(1000);
        dataLakeStreamClient.setTableName("table_name");

        // 如果你自己保存了offset 可以设置从 某个offset开始消费，有几个分区调用几次
//        dataLakeStreamClient.setPartitionCodeAndOffSet(0,100);
//        dataLakeStreamClient.setPartitionCodeAndOffSet(1,101);
//        dataLakeStreamClient.setPartitionCodeAndOffSet(2,100);
//        dataLakeStreamClient.setPartitionCodeAndOffSet(3,105);

        while (true){
            List<DataLakeStreamData> list = dataLakeStreamClient.load();

            for (DataLakeStreamData dataLakeData : list){

                System.out.println(dataLakeData);
            }
        }




    }
}
