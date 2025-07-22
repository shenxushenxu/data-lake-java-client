package com.shenxu.cn.examples;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.shenxu.cn.client.DataLakeClient;
import com.shenxu.cn.entity.BatchData;
import com.shenxu.cn.entity.LineData;

import java.util.Date;
import java.util.Map;

public class BatchInsert {
    public static void main(String[] args) throws Exception {


        int count = Integer.parseInt(args[0]);
//        int count = 50000;
        System.out.println("开始插入：：：：   " + count);

        String dataLakeIp = "hadoop101";
        int dataLakePort = 7853;

        DataLakeClient dataLakeClient = new DataLakeClient(dataLakeIp, dataLakePort);
        Gson gson = new Gson();

        BatchData batchData = new BatchData();
        batchData.setTableName("table_name");
        for (int i = 0; i < count; i++) {
            LineData lineData = new LineData();
            String mapJson = "{\"pid\" : \"3804609979663515648\",\"policytitle\" : \"关于2020年衡阳市中小企业发展专项资金拟支持项目的公示\",\"startime\" : \"2020-04-24 00:00:00.000\",\"endtime\" : \"2020-04-24 00:00:00.000\",\"companyid\" : \"2\",\"author\" : \"系统自动\",\"source\" : \"衡阳市财政局\",\"reads\" : 34,\"policykey\" : \"中小企业发展专项 中小企业发展专项资金\",\"isrecomm\" : 1,\"isvideo\" : 0,\"summary\" : null,\"states\" : 1,\"createtime\" : \"2020-07-17 17:21:46.000\",\"isdelete\" : 0,\"userguid\" : 0,\"isjiedu\" : 0,\"pageurl\" : \"https:\\/\\/www.hengyang.gov.cn\\/czj\\/xxgk\\/gzdt\\/tzgg\\/20200427\\/i1981929.html\",\"releasename\" : \"财政系统\",\"releaseid\" : \"4\",\"areaname\" : null,\"areacode\" : 0,\"arealist\" : \"100000,430000,430400,0,0\",\"gradeid\" : 3,\"gradename\" : \"市级\",\"classid\" : null,\"classname\" : null,\"suitid\" : null,\"suitname\" : null,\"provincename\" : \"湖南省\",\"provinceid\" : 430000,\"cityname\" : \"衡阳市\",\"cityid\" : 430400,\"countyname\" : null,\"countyid\" : 0,\"parentplatform\" : \"政策快车\",\"parentplatformid\" : 3479085520414310401,\"sort\" : 0,\"releasetime\" : \"2020-04-24 00:00:00.000\",\"channelid\" : 3,\"channelname\" : \"公示\",\"entranceurl\" : null,\"objectid\" : 3586354307542286336,\"objectname\" : \"牛小政\",\"istop\" : 0,\"industrycode\" : null,\"industryname\" : null,\"extracttype\" : 9,\"extractstats\" : 1,\"statisticspolicy\" : null,\"operatortime\" : \"2021-06-15 17:00:53.000\",\"policytype\" : 0,\"policytagid\" : \"0\",\"policytagname\" : \"默认\"}";
            Map<String, String> jsonMap = gson.fromJson(mapJson, new TypeToken<Map<String, String>>() {
            }.getType());
            jsonMap.put("pid", String.valueOf(i));

            lineData.setMap(jsonMap);
            // 标识 lineData 数据的操作属性
            lineData.insert();
//            lineData.delete();

            dataLakeClient.putLineData(lineData);
        }

        System.out.println("数据准备完成。。。。。。");

        long start_date = new Date().getTime();


        dataLakeClient.execute("policy_info", 0);

        // 同步插入，直接插入table_name 表
//        dataLakeClient.execute("policy_info");

        // 同步插入，指定0分区，插入table_name表内
//        dataLakeClient.execute("policy_info", "0");

        // 异步插入 ，指定0分区，插入table_name表内
//        dataLakeClient.asyncExecute("policy_info", "0");

        long end_date = new Date().getTime();

//        System.out.println("{{{{{{{{{{{{{{{{{    "+ bytes.length);
        System.out.println("总时间：      "+(end_date - start_date));

    }
}
