package com.datalake.cn.entity;

import com.datalake.cn.bincode.BinCodeSerialize;

import java.nio.charset.StandardCharsets;
import java.util.*;

public class BatchData {



    private String tableName;
    private int partitionCode = -1;
    private List<String> insertColumnName;
    private List<String[]> list;

    public BatchData(List<String> insertColumnName) throws Exception {

        insertColumnName.add(SignClass.INSERT_KEY);

        if (insertColumnName.size() == 0) {
            throw new Exception("insertColumnName size 不能为 0");
        }

        Set<String> set = new HashSet<>(insertColumnName);
        if (set.size() != insertColumnName.size()) {
            throw new Exception("insertColumnName内存在重复的列");
        }

        list = new ArrayList<>();
        this.insertColumnName = insertColumnName;
    }


    /**
     * 将 line 放置到 批中
     *
     * @param lineData
     */
    public void putLineData(LineData lineData) throws Exception {
        Map<String, Object> dataMap = lineData.getMap();

        Set<String> set = dataMap.keySet();
        for (String col : set) {
            if (!insertColumnName.contains(col)) {
                throw new Exception("插入了 insertColumnName 不存在的列：" + col);
            }
        }

        String[] valueArray = new String[this.insertColumnName.size()];
        for (int i = 0; i < valueArray.length; i++) {
            String columnName = this.insertColumnName.get(i);
            Object value = dataMap.get(columnName);
            if (value == null || "".equals(value)) {
                if (SignClass.INSERT_KEY.equals(columnName)) {
                    throw new Exception("LineData 未标识操作行为 (lineData.insert(), lineData.delete())");
                }
                value = SignClass.NULL_STR;
            }
            valueArray[i] = value.toString();
        }

        list.add(valueArray);
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public int getSize() {
        return list.size();
    }

    public void clear() {
        list.clear();
    }


    /**
     * 把数据序列化为 rust bincode的格式
     *
     * @return
     */
    public byte[] serializeToBincode() {
        BinCodeSerialize binCodeSerialize = new BinCodeSerialize();

        try {

            byte[] tableNameBytes = this.tableName.getBytes(StandardCharsets.UTF_8);
            binCodeSerialize.setLong(tableNameBytes.length);  // u64 长度前缀
            binCodeSerialize.setBytes(tableNameBytes);


            int columnSize = this.insertColumnName.size();
            // 序列化数组：长度前缀(8字节) + 元素序列化
            binCodeSerialize.setLong(columnSize);
            for (String columnName : this.insertColumnName) {
                byte[] columnBytes = columnName.getBytes(StandardCharsets.UTF_8);
                binCodeSerialize.setLong(columnBytes.length);  // u64 长度前缀
                binCodeSerialize.setBytes(columnBytes);
            }

//            System.out.println(Arrays.toString(binCodeSerialize.getBytes()));

            // 序列化数组：长度前缀(8字节) + 元素序列化
            binCodeSerialize.setLong(this.list.size());
            for (String[] valueArray : this.list) {
                binCodeSerialize.setLong(valueArray.length);
                for (String value : valueArray) {
                    byte[] colBytes = value.toString().getBytes(StandardCharsets.UTF_8);
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
