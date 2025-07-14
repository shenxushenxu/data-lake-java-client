package com.shenxu.cn.client;


import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.shenxu.cn.entity.LineData;
import com.shenxu.cn.entity.TableStructure;
import org.xerial.snappy.Snappy;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class DataLakeClient {

    private Socket socket;
    private DataOutputStream out;
    private DataInputStream in;
    //    private String tableName;
    private List data_array;



    public DataLakeClient(String DataLakeIp, int DataLakePort) throws IOException {

        data_array = new ArrayList();
        socket = new Socket(DataLakeIp, DataLakePort);
        out = new DataOutputStream(socket.getOutputStream());
        in = new DataInputStream(socket.getInputStream());
    }


    /**
     * 将 line 放置到 批中
     * @param lineData
     */
    public void putLineData(LineData lineData){
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

        byte[] data_byte = Snappy.compress(new Gson().toJson(data_array));
        List<Integer> unsignedList = new ArrayList<>();
        for (byte b : data_byte) {
            unsignedList.add(b & 0xFF); // 转换为 0-255
        }

        Map<String, Object> data = new HashMap<String, Object>();
        data.put("data", unsignedList);
        data.put("table_name", tableName);
        data.put("partition_code", partitionCode);


        Map<String, Object> batch_insert = new HashMap<String, Object>();
        batch_insert.put("batch_insert", data);
        data_array.clear();

        saveData(new Gson().toJson(batch_insert));

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

        byte[] data_byte = Snappy.compress(new Gson().toJson(data_array));
        List<Integer> unsignedList = new ArrayList<>();
        for (byte b : data_byte) {
            unsignedList.add(b & 0xFF); // 转换为 0-255
        }
        Map<String, Object> data = new HashMap<String, Object>();
        data.put("data", unsignedList);
        data.put("table_name", tableName);

        Map<String, Object> batch_insert = new HashMap<String, Object>();
        batch_insert.put("batch_insert", data);
        data_array.clear();


        saveData(new Gson().toJson(batch_insert));

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
        jsonObject.put("sql", "show " + tableName);
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

        saveData(new Gson().toJson(jsonObject));
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
