package com.datalake.cn.entity;


import com.google.gson.Gson;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;





public class DataLakeLinkData implements Serializable {


    private Map<String, Object> fields;


    @Override
    public String toString() {
        return fields.toString();
    }

    public DataLakeLinkData(){
        fields = new HashMap<String, Object>();
    }

    public void put(String column, Object value){
        fields.put(column, value.toString());
    }

    public Object get(String column){
        return fields.get(column);
    }

    public String getString(String column){
        Object value = fields.get(column);
        if (value == null){
            return null;
        }else {
            return value.toString();
        }
    }

    public Long getLong(String column){
        Object value = fields.get(column);
        if (value == null){
            return null;
        }else {
            return Long.valueOf(value.toString());
        }
    }

    public Integer getInt(String column){
        Object value = fields.get(column);
        if (value == null){
            return null;
        }else {
            return Integer.valueOf(value.toString());
        }


    }
    public Float getFloat(String column){
        Object value = fields.get(column);

        if (value == null){
            return null;
        }else {
            return Float.valueOf(value.toString());
        }


    }

    public Boolean getBoolean(String column){
        Object value = fields.get(column);

        if (value == null){
            return null;
        }else {
            return Boolean.valueOf(value.toString());
        }


    }

    public void insert(){
        fields.put(SignClass.INSERT_KEY, "insert");
    }

    public void delete(){
        fields.put(SignClass.INSERT_KEY, "delete");
    }

    public String toJSONString(){
        Gson gson = new Gson();
        return gson.toJson(fields);
    }
    public Map<String, Object> getMap(){
        return fields;
    }
    public void setMap(Map<String, Object> jsonObject){
        this.fields = jsonObject;
    }

    public int getSize(){
        return this.fields.size();
    }



}
