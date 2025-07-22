package com.shenxu.cn.entity;

import com.google.gson.annotations.SerializedName;

import java.io.Serializable;


public class DataLakeStreamData implements Serializable {
    @SerializedName("table_name")
    private String tableName;

    @SerializedName("major_value")
    private String majorValue;

    @SerializedName("_crud_type")
    private String crudType;

    @SerializedName("partition_code")
    private int partitionCode;

    @SerializedName("offset")
    private long offset;

//    @SerializedName("data")
    private LineData data;

    @Override
    public String toString() {
        return "DataLakeStreamData{" +
                "tableName=" + tableName +
                ", majorValue=" + majorValue +
                ", crudType=" + crudType +
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
