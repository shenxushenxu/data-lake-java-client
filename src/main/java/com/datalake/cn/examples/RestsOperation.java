package com.datalake.cn.examples;

import com.datalake.cn.client.DataLakeClient;

import java.util.Map;


public class RestsOperation {
    public static void main(String[] args) {
        String dataLakeIp = "12.0.0.1";
        int dataLakePort = 7853;

        try {
            DataLakeClient dataLakeClient = new DataLakeClient(dataLakeIp, dataLakePort,"table_name");
            // 获得table_name最大的offset
            Map<Integer, Long> map = dataLakeClient.getMaxOffset();
            System.out.println("map = "+map);
            // 删除table_name的 column 字段
            dataLakeClient.alterTableDelete("column");
            // 向table_name 添加 column 字段，如果column字段为null 默认值为 test
            dataLakeClient.alterTableAdd("column", "string", "test");
            // 向table_name 添加 column 字段
            dataLakeClient.alterTableAdd("column", "string");
            // 获得 table_name 的表结构 （字段名，字段类型，分区个数，主键名称，各各分区的状态）
            dataLakeClient.getTableStructure();
            // 删除table_name表
            dataLakeClient.dropTable();
            // 压缩table_name 表的数据
            dataLakeClient.compress();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
