package com.shenxu.cn.entity;

import java.io.Serializable;

public class ColumnDefinition implements Serializable {
    private String columnName;
    private ColumnConfig config;

    public ColumnDefinition(String columnName, ColumnConfig config) {
        this.columnName = columnName;
        this.config = config;
    }

    // Getters & Setters
    public String getColumnName() { return columnName; }
    public ColumnConfig getConfig() { return config; }

    @Override
    public String toString() {
        return "ColumnDefinition{" +
                "columnName='" + columnName + '\'' +
                ", config=" + config +
                '}';
    }
}
