package com.datalake.cn.entity;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;


import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TableStructure implements Serializable {


    private Map<String, List<PartitionInfo>> partitionAddress;
    private String majorKey;
    private int partitionNumber;
    private String tableName;
    private List<ColumnDefinition> column;

    public TableStructure(String tablestructure){

        Gson gson = new Gson();
        Map<String, String> jsonMap = gson.fromJson(tablestructure, new TypeToken<Map<String, String>>() { }.getType());
        String partition_address = jsonMap.get("partition_address");
        String table_name = jsonMap.get("table_name");
        String map_column = jsonMap.get("column");
        String major_key = jsonMap.get("major_key");
        String partition_number = jsonMap.get("partition_number");


        this.tableName = table_name;
        this.partitionNumber = Integer.valueOf(partition_number);
        this.majorKey = major_key;


        this.partitionAddress = gson.fromJson(partition_address, new TypeToken<Map<String, List<PartitionInfo>>>() { }.getType());

        Type mapListType = new TypeToken<List<Map<String, String>>>() {}.getType();
        List<Map<String, String>> rawList = gson.fromJson(map_column, mapListType);

        // 第二步：转换为结构化对象
        column = new ArrayList<>();
        for (Map<String, String> raw : rawList) {
            raw.forEach((columnName, configJson) -> {
                // 解析嵌套的 JSON 字符串
                ColumnConfig config = gson.fromJson(configJson, ColumnConfig.class);
                column.add(new ColumnDefinition(columnName, config));
            });
        }

    }

    public Map<String, List<PartitionInfo>> getPartitionAddress() {
        return partitionAddress;
    }

    public void setPartitionAddress(Map<String, List<PartitionInfo>> partitionAddress) {
        this.partitionAddress = partitionAddress;
    }

    public String getMajorKey() {
        return majorKey;
    }

    public void setMajorKey(String majorKey) {
        this.majorKey = majorKey;
    }

    public int getPartitionNumber() {
        return partitionNumber;
    }

    public void setPartitionNumber(int partitionNumber) {
        this.partitionNumber = partitionNumber;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<ColumnDefinition> getColumn() {
        return column;
    }

    public void setColumn(List<ColumnDefinition> column) {
        this.column = column;
    }

    @Override
    public String toString() {
        return "TableStructure{" +
                "partitionAddress=" + partitionAddress +
                ", majorKey='" + majorKey + '\'' +
                ", partitionNumber=" + partitionNumber +
                ", tableName='" + tableName + '\'' +
                ", column=" + column +
                '}';
    }
}
