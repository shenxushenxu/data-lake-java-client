package com.datalake.cn.bincode;

import com.datalake.cn.entity.SignClass;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class BinCodeDeserialize {

    private ByteBuffer buffer = null;

    public BinCodeDeserialize(byte[] bytes){
        buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
    }

    public Map<String, Object> getHashMap() {
        // 读取 HashMap 大小 (u64)
        int mapSize = buffer.getInt();
        Map<String, Object> map = new HashMap<>(mapSize);

        for (int i = 0; i < mapSize; i++) {
            String key = this.getString();
            String value = this.getString();
            if (SignClass.NULL_STR.equals(value)){
                map.put(key, null);
            }else {
                map.put(key, value);
            }
        }
        return map;
    }

    public long getLong(){
        return buffer.getLong();
    }
    public int getInt(){
        return buffer.getInt();
    }
    public String getString(){
        // 读取字符串长度 (i32)
        int len = buffer.getInt();
        byte[] bytes = new byte[len];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }








}
