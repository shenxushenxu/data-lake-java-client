package com.datalake.cn.entity;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;


import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TableStructure implements Serializable {


    private Map<String, List<PartitionInfo>> partitionAddress;
    private String majorKey;
    private int partitionNumber;
    private String tableName;
    private List<ColumnDefinition> column;

    public TableStructure(String tablestructure){
        String partitionAddressKey = "partition_address";
        String tableNameKey = "table_name";
        String majorKey = "major_key";
        String partitionNumberKey = "partition_number";

        Gson gson = new Gson();
        Map<String, String> jsonMap = gson.fromJson(tablestructure, new TypeToken<Map<String, String>>() { }.getType());
        String partition_address = jsonMap.get(partitionAddressKey);
        String table_name = jsonMap.get(tableNameKey);
        String major_key = jsonMap.get(majorKey);
        String partition_number = jsonMap.get(partitionNumberKey);


        this.tableName = table_name;
        this.partitionNumber = Integer.valueOf(partition_number);
        this.majorKey = major_key;


        this.partitionAddress = gson.fromJson(partition_address, new TypeToken<Map<String, List<PartitionInfo>>>() { }.getType());


        // 第二步：转换为结构化对象
        column = new ArrayList<>();

        Set<String> keySet = jsonMap.keySet();
        for (String key : keySet) {

            if (!partitionAddressKey.equals(key) && !tableNameKey.equals(key) && !majorKey.equals(key) && !partitionNumberKey.equals(key)){
                String configJson = jsonMap.get(key);

                ColumnConfig config = gson.fromJson(configJson, ColumnConfig.class);
                column.add(new ColumnDefinition(key, config));

            }
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
