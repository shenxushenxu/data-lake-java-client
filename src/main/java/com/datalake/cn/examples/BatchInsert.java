package com.datalake.cn.examples;

import com.datalake.cn.client.DataLakeClient;
import com.datalake.cn.entity.DataLakeLinkData;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class BatchInsert {
    public static void main(String[] args) throws Exception {


//        int count = Integer.parseInt(args[0]);
        int count = 500000;
        System.out.println("开始插入：：：：   " + count);

        String dataLakeIp = "127.0.0.1";
        int dataLakePort = 7853;

        List<String> stringList = new ArrayList<>();
        stringList.add("pid");
        stringList.add("policytitle");
        stringList.add("startime");
        stringList.add("endtime");
        stringList.add("companyid");
        stringList.add("author");
        stringList.add("source");
        stringList.add("reads");
        stringList.add("policykey");
        stringList.add("isrecomm");
        stringList.add("isvideo");
        stringList.add("summary");
        stringList.add("states");
        stringList.add("createtime");
        stringList.add("isdelete");
        stringList.add("userguid");
        stringList.add("isjiedu");
        stringList.add("pageurl");
        stringList.add("releasename");
        stringList.add("releaseid");
        stringList.add("areaname");
        stringList.add("areacode");
        stringList.add("arealist");
        stringList.add("gradeid");
        stringList.add("gradename");
        stringList.add("classid");
        stringList.add("classname");
        stringList.add("suitid");
        stringList.add("suitname");
        stringList.add("provincename");
        stringList.add("provinceid");
        stringList.add("cityname");
        stringList.add("cityid");
        stringList.add("countyname");
        stringList.add("countyid");
        stringList.add("parentplatform");
        stringList.add("parentplatformid");
        stringList.add("sort");
        stringList.add("releasetime");
        stringList.add("channelid");
        stringList.add("channelname");
        stringList.add("entranceurl");
        stringList.add("objectid");
        stringList.add("objectname");
        stringList.add("istop");
        stringList.add("industrycode");
        stringList.add("industryname");
        stringList.add("extracttype");
        stringList.add("extractstats");
        stringList.add("statisticspolicy");
        stringList.add("operatortime");
        stringList.add("policytype");
        stringList.add("policytagid");
        stringList.add("policytagname");

        DataLakeClient dataLakeClient = new DataLakeClient(dataLakeIp, dataLakePort, "policy_info", stringList);
        Gson gson = new Gson();


        for (int i = 0; i < count; i++) {
            DataLakeLinkData dataLakeLinkData = new DataLakeLinkData();
            String mapJson = "{\"pid\" : \"3804609979663515648\",\"policytitle\" : \"关于2020年衡阳市中小企业发展专项资金拟支持项目的公示\",\"startime\" : \"2020-04-24 00:00:00.000\",\"endtime\" : \"2020-04-24 00:00:00.000\",\"companyid\" : \"2\",\"author\" : \"系统自动\",\"source\" : \"衡阳市财政局\",\"reads\" : 34,\"policykey\" : \"中小企业发展专项 中小企业发展专项资金\",\"isrecomm\" : 1,\"isvideo\" : 0,\"summary\" : null,\"states\" : 1,\"createtime\" : \"2020-07-17 17:21:46.000\",\"isdelete\" : 0,\"userguid\" : 0,\"isjiedu\" : 0,\"pageurl\" : \"https:\\/\\/www.hengyang.gov.cn\\/czj\\/xxgk\\/gzdt\\/tzgg\\/20200427\\/i1981929.html\",\"releasename\" : \"财政系统\",\"releaseid\" : \"4\",\"areaname\" : null,\"areacode\" : 0,\"arealist\" : \"100000,430000,430400,0,0\",\"gradeid\" : 3,\"gradename\" : \"市级\",\"classid\" : null,\"classname\" : null,\"suitid\" : null,\"suitname\" : null,\"provincename\" : \"湖南省\",\"provinceid\" : 430000,\"cityname\" : \"衡阳市\",\"cityid\" : 430400,\"countyname\" : null,\"countyid\" : 0,\"parentplatform\" : \"政策快车\",\"parentplatformid\" : 3479085520414310401,\"sort\" : 0,\"releasetime\" : \"2020-04-24 00:00:00.000\",\"channelid\" : 3,\"channelname\" : \"公示\",\"entranceurl\" : null,\"objectid\" : 3586354307542286336,\"objectname\" : \"牛小政\",\"istop\" : 0,\"industrycode\" : null,\"industryname\" : null,\"extracttype\" : 9,\"extractstats\" : 1,\"statisticspolicy\" : null,\"operatortime\" : \"2021-06-15 17:00:53.000\",\"policytype\" : 0,\"policytagid\" : \"0\",\"policytagname\" : \"默认\"}";
            Map<String, Object> jsonMap = gson.fromJson(mapJson, new TypeToken<Map<String, Object>>() {
            }.getType());
            jsonMap.put("pid", String.valueOf(i));

            dataLakeLinkData.setMap(jsonMap);
            // 标识 lineData 数据的操作属性
            dataLakeLinkData.insert();
//            lineData.delete();

            dataLakeClient.putLineData(dataLakeLinkData);
        }

        System.out.println("数据准备完成。。。。。。");

        long start_date = new Date().getTime();

        dataLakeClient.execute();


        long end_date = new Date().getTime();

        System.out.println("总时间：      "+(end_date - start_date));

    }
}
