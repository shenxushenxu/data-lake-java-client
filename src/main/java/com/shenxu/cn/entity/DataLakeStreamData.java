package com.shenxu.cn.entity;

import com.alibaba.fastjson.JSONObject;

public class DataLakeStreamData {

    private String tableName;
    private String majorValue;
    private String crudType;
    private int partitionCode;
    private long offset;
    private LineData data;

    @Override
    public String toString() {
        return "DataLakeStreamData{" +
                "tableName='" + tableName + '\'' +
                ", majorValue='" + majorValue + '\'' +
                ", crudType='" + crudType + '\'' +
                ", partitionCode=" + partitionCode +
                ", offset=" + offset +
                ", data=" + data +
                '}';
    }

    public DataLakeStreamData(String tableName,
                              String majorValue,
                              String crudType,
                              int partitionCode,
                              long offset,
                              LineData data) {
        this.tableName = tableName;
        this.majorValue = majorValue;
        this.crudType = crudType;
        this.partitionCode = partitionCode;
        this.offset = offset;
        this.data = data;
    }

    public static DataLakeStreamData parseDataLakeStreamData(JSONObject dataLakeData) {
//        JSONObject jsonObject = JSONObject.parseObject(dataLakeData);

        String tableName = dataLakeData.getString("table_name");
        String majorValue = dataLakeData.getString("major_value");
        JSONObject jsonData = dataLakeData.getJSONObject("data");
        String crudType = dataLakeData.getString("_crud_type");
        int partitionCode = dataLakeData.getInteger("partition_code");
        long offset = dataLakeData.getLong("offset");

        LineData data = new LineData();
        data.setJsonObject(jsonData);

        return new DataLakeStreamData(tableName,
                majorValue,
                crudType,
                partitionCode,
                offset,
                data);

    }


    public String getTableName() {
        return tableName;
    }



    public String getMajorValue() {
        return majorValue;
    }



    public String getCrudType() {
        return crudType;
    }



    public int getPartitionCode() {
        return partitionCode;
    }



    public long getOffset() {
        return offset;
    }

    public LineData getData() {
        return data;
    }
}
