package com.datalake.cn.examples;

import com.datalake.cn.client.DataLakeStreamClient;
import com.datalake.cn.entity.DataLakeStreamData;

import java.util.Date;
import java.util.List;
import java.util.Map;

public class StreamConsume {
    public static void main(String[] args) throws Exception {

        int count = Integer.valueOf(args[0]);

        DataLakeStreamClient dataLakeStreamClient = new DataLakeStreamClient("hadoop101", 7853);

        dataLakeStreamClient.setReadCount(count);
        dataLakeStreamClient.setTableName("policy_info");

        // 如果你自己保存了offset 可以设置从 某个offset开始消费，有几个分区调用几次
//        dataLakeStreamClient.setPartitionCodeAndOffSet(0,0);
//        dataLakeStreamClient.setPartitionCodeAndOffSet(1,2003);
//        dataLakeStreamClient.setPartitionCodeAndOffSet(2,2003);
//        dataLakeStreamClient.setPartitionCodeAndOffSet(3,2003);
//        dataLakeStreamClient.setPartitionCodeAndOffSet(4,0);
//        dataLakeStreamClient.setPartitionCodeAndOffSet(5,2003);
//        int i = 0;
//        while (true){



        long start_time = new Date().getTime();
        List<DataLakeStreamData> list = dataLakeStreamClient.load();
        long end_time = new Date().getTime();
        System.out.println("读取的总时间：  " + (end_time - start_time));



//            Map<Integer, Long> mm = dataLakeStreamClient.getOffsetSave();


//            for (DataLakeStreamData dataLakeData : list){
//                 i ++;
//                System.out.println(mm+"  kk :   "+ i + "    "+ dataLakeData);
//            }

//            System.out.println(dataLakeStreamClient.getOffsetSave());
//        }


    }
}
