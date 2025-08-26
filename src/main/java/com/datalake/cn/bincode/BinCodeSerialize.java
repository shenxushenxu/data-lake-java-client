package com.datalake.cn.bincode;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;


public class BinCodeSerialize {


    private final ByteArrayOutputStream stream;

    // 改为实例变量（非线程安全设计）
    private final byte[] fourByteArray = new byte[4];
    private final byte[] eightByteArray = new byte[8];

//    public BinCodeSerialize(int size) {
//        stream = ByteBuffer.allocate(size);
//    }
//
//    public BinCodeSerialize() {
//        stream = ByteBuffer.allocate(500 * 1024 * 1024);
//
//    }


    public BinCodeSerialize(int size) {
        stream = new ByteArrayOutputStream(size);
    }

    public BinCodeSerialize() {
        stream = new ByteArrayOutputStream(5 * 1024 * 1024);

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

    public byte[] getLongBytes(long value) throws IOException {
        byte[] longBytes = new byte[8];
        // 小端序写入（使用无符号右移）
        longBytes[0] = (byte) value;
        longBytes[1] = (byte) (value >>> 8);
        longBytes[2] = (byte) (value >>> 16);
        longBytes[3] = (byte) (value >>> 24);
        longBytes[4] = (byte) (value >>> 32);
        longBytes[5] = (byte) (value >>> 40);
        longBytes[6] = (byte) (value >>> 48);
        longBytes[7] = (byte) (value >>> 56);
        return longBytes;
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
//        stream.flip();
//        return Arrays.copyOfRange(
//                stream.array(),
//                stream.arrayOffset() + stream.position(),
//                stream.arrayOffset() + stream.limit()
//        );

//        byte[] result = new byte[stream.remaining()];
//
//        // 将数据从堆外内存复制到堆内数组
//        stream.get(result);
//
//        // 如果需要保留原始缓冲区的位置，可以这样做：
//        int originalPosition = stream.position();
//        stream.position(0); // 或您需要的位置
//        stream.get(result);
//        stream.position(originalPosition);


//        return result;
    }

}
