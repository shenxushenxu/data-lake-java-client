package com.datalake.cn.entity;

import com.datalake.cn.bincode.BinCodeSerialize;

import java.nio.charset.StandardCharsets;
import java.util.*;

public class BatchData {

    private String tableName;
    private List<String> insertColumnName;
    private List<String[]> list;

    public BatchData(List<String> insertColumnName) throws Exception {

        insertColumnName = new ArrayList<>(insertColumnName);

        if (insertColumnName.size() == 0) {
            throw new Exception("insertColumnName size 不能为 0");
        }

        insertColumnName.add(SignClass.LINKDATA_KEY);

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
     * @param dataLakeLinkData
     */
    public void putDataLakeLinkData(DataLakeLinkData dataLakeLinkData) throws Exception {
        Map<String, Object> dataMap = dataLakeLinkData.getMap();

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
                if (SignClass.LINKDATA_KEY.equals(columnName)) {
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
            binCodeSerialize.setInt(tableNameBytes.length);  // u64 长度前缀
            binCodeSerialize.setBytes(tableNameBytes);


            int columnSize = this.insertColumnName.size();

            binCodeSerialize.setInt(columnSize);
            for (String columnName : this.insertColumnName) {
                byte[] columnBytes = columnName.getBytes(StandardCharsets.UTF_8);
                binCodeSerialize.setInt(columnBytes.length);  // u64 长度前缀
                binCodeSerialize.setBytes(columnBytes);
            }

            int listSize = this.list.size();
            binCodeSerialize.setInt(listSize);
            for (String[] valueArray : this.list) {

                binCodeSerialize.setInt(valueArray.length);

                for (String value : valueArray) {

                    byte[] colBytes = value.getBytes(StandardCharsets.UTF_8);
                    binCodeSerialize.setInt(colBytes.length);  // u64 长度前缀
                    binCodeSerialize.setBytes(colBytes);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return binCodeSerialize.getBytes();
    }


}
