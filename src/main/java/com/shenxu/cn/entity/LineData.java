package com.shenxu.cn.entity;

import com.alibaba.fastjson.JSONObject;

public class LineData {
    private JSONObject jsonObject;

    @Override
    public String toString() {
        return "LineData{" + jsonObject + '}';
    }

    public LineData(){
        jsonObject = new JSONObject();
    }

    public void put(String column, Object value){
        jsonObject.put(column, value);
    }

    public Object get(String column){
        return jsonObject.get(column);
    }

    public String getString(String column){
        return jsonObject.get(column).toString();
    }

    public Long getLong(String column){
        String value = jsonObject.get(column).toString();
        return Long.valueOf(value);
    }

    public Integer getInt(String column){
        String value = jsonObject.get(column).toString();
        return Integer.valueOf(value);
    }
    public Float getFloat(String column){
        String value = jsonObject.get(column).toString();
        return Float.valueOf(value);
    }

    public Boolean getBoolean(String column){
        String value = jsonObject.get(column).toString();
        return Boolean.valueOf(value);
    }

    public String toJSONString(){
        return jsonObject.toJSONString();
    }
    public JSONObject getJsonObject(){
        return jsonObject;
    }
    public void setJsonObject(JSONObject jsonObject){
        this.jsonObject = jsonObject;
    }



}
