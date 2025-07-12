package com.shenxu.cn.client;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.shenxu.cn.entity.LineData;
import org.xerial.snappy.Snappy;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.rmi.server.ExportException;
import java.util.ArrayList;
import java.util.List;


public class DataLakeClient {

    private Socket socket;
    private DataOutputStream out;
    private DataInputStream in;
    //    private String tableName;
    private JSONArray data_array;
    private String insert_key = "_crud_type";


    public DataLakeClient(String DataLakeIp, int DataLakePort) throws IOException {

        data_array = new JSONArray();
        socket = new Socket(DataLakeIp, DataLakePort);
        out = new DataOutputStream(socket.getOutputStream());
        in = new DataInputStream(socket.getInputStream());
    }

    /**
     * 设置表需要插入表的名字
     * @param tableName
     */
//    public void setTableName(String tableName) {
//        this.tableName = tableName;
//    }

    /**
     * 如果是insert数据就把LineData 的对象放到这里面
     *
     * @param lineData
     */
    public void insert(LineData lineData) {

        lineData.put(insert_key, "insert");
        data_array.add(lineData.getJsonObject());
    }

    /**
     * 如果是delete数据就把LineData 的对象放到这里面
     *
     * @param lineData
     */
    public void delete(LineData lineData) {
        lineData.put(insert_key, "delete");
        data_array.add(lineData.toJSONString());
    }

    /**
     * 执行插入的方法
     *
     * @param tableName     指定插入的表名
     * @param partitionCode 指定插入的分区号
     * @throws Exception
     */
    public void execute(String tableName, String partitionCode) throws Exception {

        if (data_array.size() == 0) {
            throw new Exception("data_array内无数据可插入");
        }
        if (tableName == null || "".equals(tableName)) {
            throw new Exception("tableName 为 null");
        }

        byte[] data_byte = Snappy.compress(data_array.toJSONString());
        List<Integer> unsignedList = new ArrayList<>();
        for (byte b : data_byte) {
            unsignedList.add(b & 0xFF); // 转换为 0-255
        }

        JSONObject data = new JSONObject();
        data.put("data", unsignedList);
        data.put("table_name", tableName);
        data.put("partition_code", partitionCode);


        JSONObject batch_insert = new JSONObject();
        batch_insert.put("batch_insert", data);
        data_array.clear();


        saveData(batch_insert.toJSONString());

    }

    /**
     * 执行插入的方法
     *
     * @param tableName 指定插入的表名
     * @throws Exception
     */
    public void execute(String tableName) throws Exception {

        if (data_array.size() == 0) {
            throw new Exception("data_array内无数据可插入");
        }
        if (tableName == null || "".equals(tableName)) {
            throw new Exception("tableName 为 null");
        }

        byte[] data_byte = Snappy.compress(data_array.toJSONString());
        List<Integer> unsignedList = new ArrayList<>();
        for (byte b : data_byte) {
            unsignedList.add(b & 0xFF); // 转换为 0-255
        }
        JSONObject data = new JSONObject();
        data.put("data", unsignedList);
        data.put("table_name", tableName);

        JSONObject batch_insert = new JSONObject();
        batch_insert.put("batch_insert", data);
        data_array.clear();


        saveData(batch_insert.toJSONString());

    }


    /**
     * 获得表结构
     *
     * @param tableName
     * @return
     * @throws Exception
     */
    public JSONObject getTableStructure(String tableName) throws Exception {

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("sql", "show " + tableName);
        String data = jsonObject.toJSONString();
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
            return JSONObject.parseObject(tableStructure);
        }

    }

    /**
     * 删除表
     */
    public int dropTable(String tableName) throws Exception {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("sql", "drop " + tableName);
        String data = jsonObject.toJSONString();
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

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("sql", "ALTER TABLE "
                + tableName + " ADD "
                + column + " "
                + type + " "
                + "DEFAULT " + defaultValue);
        String data = jsonObject.toJSONString();
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

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("sql", "ALTER TABLE " + tableName + " ADD " + column + " " + type);
        String data = jsonObject.toJSONString();
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

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("sql", "ALTER TABLE "+tableName+" OROP "+column);
        String data = jsonObject.toJSONString();
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
    public JSONObject getMaxOffset(String tableName) throws Exception {

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("sql", "MAX_OFFSET "+tableName);
        String data = jsonObject.toJSONString();
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
            return JSONObject.parseObject(offsets);
        }
    }

    /**
     * 对 datalake内的表进行压缩
     *
     * @throws Exception
     */
    public void compress(String tableName) throws Exception {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("compress_table", tableName);

        saveData(jsonObject.toJSONString());
    }

    /**
     * 向datalake 发送数据
     *
     * @param data
     * @throws Exception
     */
    private void saveData(String data) throws Exception {
        byte[] bytesData = data.getBytes();

        out.writeInt(bytesData.length);
        out.write(bytesData);
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
