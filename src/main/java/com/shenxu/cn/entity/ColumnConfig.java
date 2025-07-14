package com.shenxu.cn.entity;

import com.google.gson.annotations.SerializedName;

public class ColumnConfig {
    private String datatype;
    @SerializedName("column_config")
    private String columnConfig;
    @SerializedName("default_value")
    private String defaultValue; // 注意：某些字段可能无此属性

    // Getters & Setters
    public String getDatatype() { return datatype; }
    public void setDatatype(String datatype) { this.datatype = datatype; }
    public String getColumnConfig() { return columnConfig; }
    public void setColumnConfig(String columnConfig) { this.columnConfig = columnConfig; }
    public String getDefaultValue() { return defaultValue; }
    public void setDefaultValue(String defaultValue) { this.defaultValue = defaultValue; }


    @Override
    public String toString() {
        return "ColumnConfig{" +
                "datatype='" + datatype + '\'' +
                ", columnConfig='" + columnConfig + '\'' +
                ", defaultValue='" + defaultValue + '\'' +
                '}';
    }
}
