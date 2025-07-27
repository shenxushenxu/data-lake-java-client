package com.shenxu.cn.entity;

import com.shenxu.cn.bincode.BinCodeSerialize;

import java.nio.charset.StandardCharsets;
import java.util.*;

public class BatchData {
    private String tableName;
    private int partitionCode = -1;
    private Set<String> column = new HashSet<>();
    private List<Map<String, String>> list;
    public BatchData(){
        list = new ArrayList<>();
    }


    /**
     * 将 line 放置到 批中
     * @param lineData
     */
    public void putLineData(LineData lineData){
        Map<String, String> dataMap = lineData.getMap();

        Set<String> set = dataMap.keySet();
        for (String col : set){
            column.add(col);
        }
        list.add(lineData.getMap());
    }

    public void setTableName(String tableName){
        this.tableName = tableName;
    }
    public void setPartitionCode(int partitionCode){
        this.partitionCode = partitionCode;
    }
    public int getSize(){
        return list.size();
    }

    public void clear(){
        list.clear();
    }



    /**
     * 把数据序列化为 rust bincode的格式
     * @return
     */
    public byte[] serializeToBincode() {
        BinCodeSerialize binCodeSerialize = new BinCodeSerialize();

        try {

            byte[] tableNameBytes = this.tableName.getBytes(StandardCharsets.UTF_8);
            binCodeSerialize.setLong(tableNameBytes.length);  // u64 长度前缀
            binCodeSerialize.setBytes(tableNameBytes);

            int columnSize = this.column.size();
            String[] columnArray = new String[columnSize];
            // 序列化数组：长度前缀(8字节) + 元素序列化
            binCodeSerialize.setLong(columnSize);
            int i = 0;
            for (String col : this.column) {
                byte[] colBytes = col.getBytes(StandardCharsets.UTF_8);
                binCodeSerialize.setLong(colBytes.length);  // u64 长度前缀
                binCodeSerialize.setBytes(colBytes);
                columnArray[i] = col;
                i++;
            }

            // 序列化数组：长度前缀(8字节) + 元素序列化
            binCodeSerialize.setLong(this.list.size());
            for (Map<String, String> map : this.list) {
                binCodeSerialize.setLong(map.size());
                for (String col : columnArray){
                    String value = map.get(col);
                    if (value == null || "".equals(value)){
                        value = "-1";
                    }
                    byte[] colBytes = value.getBytes(StandardCharsets.UTF_8);
                    binCodeSerialize.setLong(colBytes.length);  // u64 长度前缀
                    binCodeSerialize.setBytes(colBytes);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return binCodeSerialize.getBytes();
    }















}
