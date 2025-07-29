package com.shenxu.cn.client;


import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.shenxu.cn.entity.BatchData;
import com.shenxu.cn.entity.LineData;
import com.shenxu.cn.entity.TableStructure;
import org.xerial.snappy.Snappy;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.net.Socket;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;


public class DataLakeClient implements Serializable {

    private Socket socket;
    private DataOutputStream out;
    private DataInputStream in;
    //    private String tableName;
    private BatchData batchData;

    public int getDataArrayLength(){
        return batchData.getSize();
    }


    public DataLakeClient(String DataLakeIp, int DataLakePort) throws IOException {
        batchData = new BatchData();
        socket = new Socket(DataLakeIp, DataLakePort);
        out = new DataOutputStream(socket.getOutputStream());
        in = new DataInputStream(socket.getInputStream());
    }


    /**
     * 将 line 放置到 批中
     * @param lineData
     */
    public void putLineData(LineData lineData){

        batchData.putLineData(lineData);
    }

    /**
     * 执行插入的方法
     *
     * @param tableName     指定插入的表名
     * @param partitionCode 指定插入的分区号
     * @throws Exception
     */
    public void execute(String tableName, int partitionCode) throws Exception {

        if (batchData.getSize() == 0) {
//            throw new Exception("data_array内无数据可插入");
            return;
        }
        if (tableName == null || "".equals(tableName)) {
            throw new Exception("tableName 为 null");
        }

        batchData.setTableName(tableName);
        batchData.setPartitionCode(partitionCode);

        byte[] dataArrayString = batchData.serializeToBincode();

        byte[] data_byte = Snappy.compress(dataArrayString);

        int dataByteLen = data_byte.length;
        byte[] unsignedList = new byte[dataByteLen];

        for (int i =0;i<dataByteLen;i++) {
            unsignedList[i] = (byte) (data_byte[i] & 0xFF);
        }
        batchData.clear();

        saveData(unsignedList);


    }

    /**
     * 执行插入的方法
     *
     * @param tableName 指定插入的表名
     * @throws Exception
     */
    public void execute(String tableName) throws Exception {

        if (batchData.getSize() == 0) {
            throw new Exception("data_array内无数据可插入");
        }
        if (tableName == null || "".equals(tableName)) {
            throw new Exception("tableName 为 null");
        }

        batchData.setTableName(tableName);

        long start_time = new Date().getTime();
        byte[] dataArrayString = batchData.serializeToBincode();
        byte[] data_byte = Snappy.compress(dataArrayString);
        int dataByteLen = data_byte.length;
        byte[] unsignedList = new byte[dataByteLen];

        for (int i =0;i<dataByteLen;i++) {
            unsignedList[i] = (byte) (data_byte[i] & 0xFF);
        }
        batchData.clear();


        long end_time = new Date().getTime();
        System.out.println("+++++++++++++++++++++   "+(end_time - start_time));

        System.out.println("传输的总数据量："+dataArrayString.length +"   压缩后的数据量为："+unsignedList.length);
        long savedata_start_time = new Date().getTime();
        saveData(unsignedList);
        long savedata_end_time = new Date().getTime();
        System.out.println("--------------------------   "+(savedata_end_time - savedata_start_time));
    }









    /**
     * 获得表结构
     *
     * @param tableName
     * @return
     * @throws Exception
     */
    public TableStructure getTableStructure(String tableName) throws Exception {

        Map<String, String> jsonObject = new HashMap<>();
        jsonObject.put("sql", "desc " + tableName);
        String data = new Gson().toJson(jsonObject);
        out.write(data.getBytes());
        out.flush();
        int is = in.readInt();
        if (is == -2) {
            int len = in.readInt();
            byte[] mess = new byte[len];
            in.readFully(mess);
            throw new Exception(new String(mess));
        } else {
            int len = in.readInt();
            byte[] mess = new byte[len];
            in.readFully(mess);
            String tableStructure = new String(mess);
            return new TableStructure(tableStructure);
        }

    }

    /**
     * 删除表
     */
    public int dropTable(String tableName) throws Exception {
        Map<String, String> jsonObject = new HashMap<>();
        jsonObject.put("sql", "drop " + tableName);
        String data = new Gson().toJson(jsonObject);
        out.write(data.getBytes());
        out.flush();
        int is = in.readInt();
        if (is == -2) {
            int len = in.readInt();
            byte[] mess = new byte[len];
            in.readFully(mess);
            throw new Exception(new String(mess));
        } else {
            return 1;
        }
    }


    /**
     * 添加列 有默认值的
     */
    public int alterTableAdd(String tableName,
                             String column,
                             String type,
                             String defaultValue) throws Exception {

        Map<String,String> jsonObject = new HashMap<>();
        jsonObject.put("sql", "ALTER TABLE "
                + tableName + " ADD "
                + column + " "
                + type + " "
                + "DEFAULT " + defaultValue);
        String data = new Gson().toJson(jsonObject);
        out.write(data.getBytes());
        out.flush();
        int is = in.readInt();
        if (is == -2) {
            int len = in.readInt();
            byte[] mess = new byte[len];
            in.readFully(mess);
            throw new Exception(new String(mess));
        } else {
            return 1;
        }
    }

    /**
     * 添加列 无默认值的
     */
    public int alterTableAdd(String tableName,
                             String column,
                             String type
    ) throws Exception {

        Map<String, String> jsonObject = new HashMap<>();
        jsonObject.put("sql", "ALTER TABLE " + tableName + " ADD " + column + " " + type);
        String data = new Gson().toJson(jsonObject);
        out.write(data.getBytes());
        out.flush();
        int is = in.readInt();
        if (is == -2) {
            int len = in.readInt();
            byte[] mess = new byte[len];
            in.readFully(mess);
            throw new Exception(new String(mess));
        } else {
            return 1;
        }
    }

    /**
     * 添加列 无默认值的
     */
    public int alterTableDelete(String tableName,
                                String column
    ) throws Exception {

        Map<String, String> jsonObject = new HashMap<>();
        jsonObject.put("sql", "ALTER TABLE "+tableName+" OROP "+column);
        String data = new Gson().toJson(jsonObject);
        out.write(data.getBytes());
        out.flush();
        int is = in.readInt();
        if (is == -2) {
            int len = in.readInt();
            byte[] mess = new byte[len];
            in.readFully(mess);
            throw new Exception(new String(mess));
        } else {
            return 1;
        }
    }


    /**
     * 获得表内每个分区最大的offset
     */
    public Map<Integer, Long> getMaxOffset(String tableName) throws Exception {

        Map<String, String> jsonObject = new HashMap<>();
        jsonObject.put("sql", "MAX_OFFSET "+tableName);
        String data = new Gson().toJson(jsonObject);
        out.write(data.getBytes());
        out.flush();
        int is = in.readInt();
        if (is == -2) {
            int len = in.readInt();
            byte[] mess = new byte[len];
            in.readFully(mess);
            throw new Exception(new String(mess));
        } else {
            int len = in.readInt();
            byte[] mess = new byte[len];
            in.readFully(mess);
            String offsets = new String(mess);
            Map<Integer, Long> jsonMap = new Gson().fromJson(offsets, new TypeToken<Map<Integer, Long>>() { }.getType());


            return jsonMap;
        }
    }

    /**
     * 对 datalake内的表进行压缩
     *
     * @throws Exception
     */
    public void compress(String tableName) throws Exception {
        Map<String, String > jsonObject = new HashMap<>();
        jsonObject.put("compress_table", tableName);
        byte[] data = new Gson().toJson(jsonObject).getBytes();
        out.writeInt(data.length);
        out.write(data);
        out.flush();
        int is = in.readInt();
        if (is == -2) {
            int len = in.readInt();
            byte[] mess = new byte[len];
            in.readFully(mess);
            throw new Exception(new String(mess));
        }
    }

    /**
     * 向datalake 发送数据
     *
     * @param data
     * @throws Exception
     */
    private void saveData(byte[] data) throws Exception {
        byte[] bi = "\"batch_insert\"".getBytes();
        out.writeInt(bi.length);
        out.write(bi);
        System.out.println("发送了  batch_insert");

        out.writeInt(data.length);
        System.out.println("1111111111111111");
        out.write(data);
        System.out.println("发送了数据");
        out.flush();

        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
        System.out.println("客户端发送数据完成的时间：    "+now.format(formatter));

        int is = in.readInt();

        if (is == -2) {

            int len = in.readInt();
            byte[] mess = new byte[len];

            in.readFully(mess);

            throw new Exception(new String(mess));
        }
    }




    /**
     * 关闭连接
     */
    public void close() throws IOException {
        out.close();
        in.close();
        socket.close();
    }
}
