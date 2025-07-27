package com.shenxu.cn.bincode;

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

    public Map<String, String> getHashMap() {
        // 读取 HashMap 大小 (u64)
        long mapSize = buffer.getLong();
        Map<String, String> map = new HashMap<>((int) mapSize);

        for (int i = 0; i < mapSize; i++) {
            String key = this.getString();
            String value = this.getString();
            map.put(key, value);
        }

        return map;
    }

    public long getLong(){
        return buffer.getLong();
    }
    public String getString(){
        // 读取字符串长度 (u64)
        long len = buffer.getLong();
        byte[] bytes = new byte[(int) len];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }








}
