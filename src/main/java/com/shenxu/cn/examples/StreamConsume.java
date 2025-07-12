package com.shenxu.cn.examples;

import com.shenxu.cn.client.DataLakeStreamClient;
import com.shenxu.cn.entity.DataLakeStreamData;

import java.util.List;

public class StreamConsume {
    public static void main(String[] args) throws Exception {

        DataLakeStreamClient dataLakeStreamClient = new DataLakeStreamClient("12.0.0.1", 7853);

        dataLakeStreamClient.setReadCount(1000);
        dataLakeStreamClient.setTableName("table_name");

        while (true){
            List<DataLakeStreamData> list = dataLakeStreamClient.load();

            for (DataLakeStreamData dataLakeData : list){


                System.out.println(dataLakeData);
            }
        }




    }
}
