package com.datalake.cn.client;


import com.datalake.cn.entity.BatchData;
import com.datalake.cn.entity.DataLakeLinkData;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.datalake.cn.entity.TableStructure;
import org.xerial.snappy.Snappy;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.net.Socket;
import java.util.*;


public class DataLakeClient implements Serializable {

    private Socket socket;
    private DataOutputStream out;
    private DataInputStream in;
    //    private String tableName;
    private BatchData batchData;

    private String tableName;

    public int getDataArrayLength(){
        return batchData.getSize();
    }

    public DataLakeClient(String DataLakeIp, int DataLakePort, String tableName) throws Exception {

        socket = new Socket(DataLakeIp, DataLakePort);
        out = new DataOutputStream(socket.getOutputStream());
        in = new DataInputStream(socket.getInputStream());
        this.tableName = tableName;
    }

    public DataLakeClient(String DataLakeIp, int DataLakePort, String tableName, List<String> insertColumName) throws Exception {
        batchData = new BatchData(insertColumName);
        batchData.setTableName(tableName);

        socket = new Socket(DataLakeIp, DataLakePort);
        out = new DataOutputStream(socket.getOutputStream());
        in = new DataInputStream(socket.getInputStream());
        this.tableName = tableName;
    }


    /**
     * 将 line 放置到 批中
     * @param dataLakeLinkData
     */
    public void putLineData(DataLakeLinkData dataLakeLinkData) throws Exception {

        batchData.putLineData(dataLakeLinkData);
    }


    /**
     * 执行插入的方法
     *
     * @throws Exception
     */
    public void execute() throws Exception {

        if (batchData.getSize() == 0) {
            return;
        }
        if (tableName == null || "".equals(tableName)) {
            throw new Exception("tableName 为 null");
        }

        byte[] dataArrayString = batchData.serializeToBincode();

        byte[] data_byte = Snappy.compress(dataArrayString);

        batchData.clear();
        saveData(data_byte);

    }









    /**
     * 获得表结构
     *
     * @param
     * @return
     * @throws Exception
     */
    public TableStructure getTableStructure() throws Exception {

        Map<String, String> jsonObject = new HashMap<>();
        jsonObject.put("sql", "desc " + tableName);
        String data = new Gson().toJson(jsonObject);
        byte[] bytes = data.getBytes();
        out.writeInt(bytes.length);
        out.write(bytes);
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
    public int dropTable() throws Exception {
        Map<String, String> jsonObject = new HashMap<>();
        jsonObject.put("sql", "drop " + tableName);
        String data = new Gson().toJson(jsonObject);
        byte[] bytes = data.getBytes();
        out.writeInt(bytes.length);
        out.write(bytes);
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
    public int alterTableAdd(String column,
                             String type,
                             String defaultValue) throws Exception {

        Map<String,String> jsonObject = new HashMap<>();
        jsonObject.put("sql", "ALTER TABLE "
                + tableName + " ADD "
                + column + " "
                + type + " "
                + "DEFAULT " + defaultValue);
        String data = new Gson().toJson(jsonObject);
        byte[] bytes = data.getBytes();
        out.writeInt(bytes.length);
        out.write(bytes);
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
    public int alterTableAdd(String column,
                             String type
    ) throws Exception {

        Map<String, String> jsonObject = new HashMap<>();
        jsonObject.put("sql", "ALTER TABLE " + tableName + " ADD " + column + " " + type);
        String data = new Gson().toJson(jsonObject);
        byte[] bytes = data.getBytes();
        out.writeInt(bytes.length);
        out.write(bytes);
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
    public int alterTableDelete( String column
    ) throws Exception {

        Map<String, String> jsonObject = new HashMap<>();
        jsonObject.put("sql", "ALTER TABLE "+tableName+" OROP "+column);
        String data = new Gson().toJson(jsonObject);
        byte[] bytes = data.getBytes();
        out.writeInt(bytes.length);
        out.write(bytes);
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
    public Map<Integer, Long> getMaxOffset() throws Exception {

        Map<String, String> jsonObject = new HashMap<>();
        jsonObject.put("sql", "MAX_OFFSET "+tableName);
        String data = new Gson().toJson(jsonObject);
        byte[] bytes = data.getBytes();
        out.writeInt(bytes.length);
        out.write(bytes);
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
    public void compress() throws Exception {
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
     * 关闭连接
     */
    public void close() throws IOException {
        out.close();
        in.close();
        socket.close();
    }
}
