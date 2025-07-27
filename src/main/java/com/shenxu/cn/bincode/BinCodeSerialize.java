package com.shenxu.cn.bincode;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class BinCodeSerialize {
    private ByteArrayOutputStream stream = null;

    public BinCodeSerialize(){
        stream =  new ByteArrayOutputStream();
    }




    public void  setInt(int value) throws IOException {
        byte[] bytes = ByteBuffer.allocate(4)
                .order(ByteOrder.LITTLE_ENDIAN)
                .putInt(value)
                .array();
        stream.write(bytes);

    }

    public void setLong(long value) throws IOException {
        byte[] bytes = ByteBuffer.allocate(8)
                .order(ByteOrder.LITTLE_ENDIAN)
                .putLong(value)
                .array();
        stream.write(bytes);
    }

    public void setFloat(float value) throws IOException {
        byte[] bytes =  ByteBuffer.allocate(4)
                .order(ByteOrder.LITTLE_ENDIAN)
                .putFloat(value)
                .array();
        stream.write(bytes);
    }

    public void setBytes(byte[] bytes) throws IOException {
        stream.write(bytes);
    }

    public byte[] getBytes(){
        return stream.toByteArray();
    }





}
