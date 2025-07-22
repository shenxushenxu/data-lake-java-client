package com.shenxu.cn.entity;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
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
        ByteArrayOutputStream stream = new ByteArrayOutputStream();

        try {
            byte[] tableNameBytes = this.tableName.getBytes(StandardCharsets.UTF_8);
            stream.write(longToBytes(tableNameBytes.length));  // u64 长度前缀
            stream.write(tableNameBytes);
            // 序列化整数 (4字节小端序)
            if (partitionCode == -1){
                stream.write(0x00);
            }else {
                stream.write(0x01);
                stream.write(intToBytes(this.partitionCode));
            }


            int columnSize = this.column.size();
            String[] columnArray = new String[columnSize];
            // 序列化数组：长度前缀(8字节) + 元素序列化
            stream.write(longToBytes(columnSize));
            int i = 0;
            for (String col : this.column) {
                byte[] colBytes = col.getBytes(StandardCharsets.UTF_8);
                stream.write(longToBytes(colBytes.length));  // u64 长度前缀
                stream.write(colBytes);
                columnArray[i] = col;
                i++;
            }

            // 序列化数组：长度前缀(8字节) + 元素序列化
            stream.write(longToBytes(this.list.size()));
            for (Map<String, String> map : this.list) {
                stream.write(longToBytes(map.size()));
                for (String col : columnArray){
                    String value = map.get(col);
                    if (value == null || "".equals(value)){
                        value = "-1";
                    }
                    byte[] colBytes = value.getBytes(StandardCharsets.UTF_8);
                    stream.write(longToBytes(colBytes.length));  // u64 长度前缀
                    stream.write(colBytes);
                }
            }


        } catch (Exception e) {
            e.printStackTrace();
        }

        return stream.toByteArray();
    }

    private static byte[] intToBytes(int value) {
        return ByteBuffer.allocate(4)
                .order(ByteOrder.LITTLE_ENDIAN)
                .putInt(value)
                .array();
    }

    private static byte[] longToBytes(long value) {
        return ByteBuffer.allocate(8)
                .order(ByteOrder.LITTLE_ENDIAN)
                .putLong(value)
                .array();
    }

    private static byte[] floatToBytes(float value) {
        return ByteBuffer.allocate(4)
                .order(ByteOrder.LITTLE_ENDIAN)
                .putFloat(value)
                .array();
    }













}
