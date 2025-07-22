package com.shenxu.cn.entity;


import com.google.gson.Gson;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class LineData implements Serializable {
    private Map<String, String> fields;


    private String insert_key = "_crud_type";

    @Override
    public String toString() {
        return "LineData{" + fields + '}';
    }

    public LineData(){
        fields = new HashMap<String, String>();
    }

    public void put(String column, Object value){
        fields.put(column, value.toString());
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

        return gson.toJson(fields);
    }
    public Map<String, String> getMap(){
        return fields;
    }
    public void setMap(Map<String, String> jsonObject){
        this.fields = jsonObject;
    }



}
