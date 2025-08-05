package com.datalake.cn.entity;


public class DataLakeStreamData {
    private String tableName;

    private String majorValue;

    private String crudType;

    private int partitionCode;

    private long offset;

//    @SerializedName("data")
    private DataLakeLinkData data;

    @Override
    public String toString() {
        return "DataLakeStreamData{" +
                "tableName=" + tableName +
                ", majorValue=" + majorValue +
                ", crudType=" + crudType +
                ", partitionCode=" + partitionCode +
                ", offset=" + offset +
                ", data=LineData" + data +
                '}';
    }



    public DataLakeStreamData(String tableName,
                              String majorValue,
                              String crudType,
                              int partitionCode,
                              long offset,
                              DataLakeLinkData data) {
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

    public DataLakeLinkData getData() {
        return data;
    }



}
