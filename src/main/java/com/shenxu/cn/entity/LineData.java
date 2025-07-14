package com.shenxu.cn.entity;


import com.google.gson.Gson;

import java.util.HashMap;
import java.util.Map;

public class LineData {
    private Map<String, Object> fields;


    private String insert_key = "_crud_type";

    @Override
    public String toString() {
        return "LineData{" + fields + '}';
    }

    public LineData(){
        fields = new HashMap<String, Object>();
    }

    public void put(String column, Object value){
        fields.put(column, value);
    }

    public Object get(String column){
        return fields.get(column);
    }

    public String getString(String column){
        return fields.get(column).toString();
    }

    public Long getLong(String column){
        String value = fields.get(column).toString();
        return Long.valueOf(value);
    }

    public Integer getInt(String column){
        String value = fields.get(column).toString();
        return Integer.valueOf(value);
    }
    public Float getFloat(String column){
        String value = fields.get(column).toString();
        return Float.valueOf(value);
    }

    public Boolean getBoolean(String column){
        String value = fields.get(column).toString();
        return Boolean.valueOf(value);
    }

    public void insert(){
        fields.put(insert_key, "insert");
    }

    public void delete(){
        fields.put(insert_key, "delete");
    }

    public String toJSONString(){
        Gson gson = new Gson();

        return gson.toJson(gson);
    }
    public Map<String, Object> getMap(){
        return fields;
    }
    public void setMap(Map<String, Object> jsonObject){
        this.fields = jsonObject;
    }



}
