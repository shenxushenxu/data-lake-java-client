package com.shenxu.cn.examples;

import com.shenxu.cn.client.DataLakeClient;
import com.shenxu.cn.entity.LineData;

import java.util.Date;

public class BatchInsert {
    public static void main(String[] args) throws Exception {


        int count = Integer.parseInt(args[0]);

//        int count = 10;
        System.out.println("开始插入：：：：   "+ count);

        String dataLakeIp = "hadoop101";
        int dataLakePort = 7853;

        DataLakeClient dataLakeClient = new DataLakeClient(dataLakeIp, dataLakePort);

        for (int i = 0; i < count; i++){
            LineData lineData = new LineData();
            lineData.put("id", String.valueOf(i));
            lineData.put("username", "data-lake");
            lineData.put("age", String.valueOf(i));
            lineData.put("xingbie", String.valueOf(i));
            // 标识 lineData 数据的操作属性
            lineData.insert();
//            lineData.delete();

            dataLakeClient.putLineData(lineData);
        }

        System.out.println("数据准备完成。。。。。。");
        long start_date = new Date().getTime();
        // 直接插入table_name 表
        dataLakeClient.execute("table_name");

        // 指定0分区，插入table_name表内
        dataLakeClient.execute("table_name", "0");

        long end_date = new Date().getTime();

        System.out.println(end_date - start_date);



    }
}
