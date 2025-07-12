package com.shenxu.cn.examples;

import com.shenxu.cn.client.DataLakeClient;


public class RestsOperation {
    public static void main(String[] args) {
        String dataLakeIp = "hadoop101";
        int dataLakePort = 7853;

        try {
            DataLakeClient dataLakeClient = new DataLakeClient(dataLakeIp, dataLakePort);
            // 获得table_name最大的offset
            dataLakeClient.getMaxOffset("table_name");
            // 删除table_name的 column 字段
            dataLakeClient.alterTableDelete("table_name","column");
            // 向table_name 添加 column 字段，如果column字段为null 默认值为 test
            dataLakeClient.alterTableAdd("table_name", "column", "string", "test");
            // 向table_name 添加 column 字段
            dataLakeClient.alterTableAdd("table_name", "column", "string");
            // 获得 table_name 的表结构 （字段名，字段类型，分区个数，主键名称，各各分区的状态）
            dataLakeClient.getTableStructure("table_name");
            // 删除table_name表
            dataLakeClient.dropTable("table_name");
            // 压缩table_name 表的数据
            dataLakeClient.compress("table_name");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
