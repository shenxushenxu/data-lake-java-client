package com.datalake.cn.bincode;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;


public class BinCodeSerialize {
    private final ByteArrayOutputStream stream;

    // 改为实例变量（非线程安全设计）
    private final byte[] fourByteArray = new byte[4];
    private final byte[] eightByteArray = new byte[8];

    public BinCodeSerialize() {
        stream = new ByteArrayOutputStream();
    }

    public void setInt(int value) throws IOException {
        // 小端序写入（去除冗余位与操作）
        fourByteArray[0] = (byte) value;
        fourByteArray[1] = (byte) (value >>> 8);
        fourByteArray[2] = (byte) (value >>> 16);
        fourByteArray[3] = (byte) (value >>> 24);
        stream.write(fourByteArray, 0, 4);
    }

    public void setLong(long value) throws IOException {
        // 小端序写入（使用无符号右移）
        eightByteArray[0] = (byte) value;
        eightByteArray[1] = (byte) (value >>> 8);
        eightByteArray[2] = (byte) (value >>> 16);
        eightByteArray[3] = (byte) (value >>> 24);
        eightByteArray[4] = (byte) (value >>> 32);
        eightByteArray[5] = (byte) (value >>> 40);
        eightByteArray[6] = (byte) (value >>> 48);
        eightByteArray[7] = (byte) (value >>> 56);
        stream.write(eightByteArray, 0, 8);
    }

    public void setFloat(float value) throws IOException {
        // 内联setInt逻辑避免方法调用开销
        int intValue = Float.floatToIntBits(value);
        fourByteArray[0] = (byte) intValue;
        fourByteArray[1] = (byte) (intValue >>> 8);
        fourByteArray[2] = (byte) (intValue >>> 16);
        fourByteArray[3] = (byte) (intValue >>> 24);
        stream.write(fourByteArray, 0, 4);
    }

    public void setBytes(byte[] bytes) throws IOException {
        stream.write(bytes, 0, bytes.length);
    }

    public byte[] getBytes() {
        return stream.toByteArray();
    }
}
