package com.shenxu.cn.client;


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.shenxu.cn.deserializer.DataLakeStreamDataDeserializer;
import com.shenxu.cn.entity.DataLakeStreamData;

import org.xerial.snappy.Snappy;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.net.Socket;
import java.util.*;

public class DataLakeStreamClient implements Serializable {

    private Socket socket;
    private DataOutputStream out;
    private DataInputStream in;
    private List<Map<String, Long>> PartitionCodeAndOffSet;
    private String TableName;
    public Map<Integer, Long> offsetSave;

    private Gson gson;
    private int readCount;

    public DataLakeStreamClient(String serverAddress, int serverPort) throws IOException {
        socket = new Socket(serverAddress, serverPort);
        out = new DataOutputStream(socket.getOutputStream());
        in = new DataInputStream(socket.getInputStream());

        this.PartitionCodeAndOffSet = new ArrayList();
        this.offsetSave = new HashMap<Integer, Long>();

        this.gson = new GsonBuilder()
                .registerTypeAdapter(DataLakeStreamData.class, new DataLakeStreamDataDeserializer())
                .create();


    }


    public void setTableName(String tableName) {
        this.TableName = tableName;

    }

    public void setReadCount(int readCount) {
        this.readCount = readCount;

    }

    private void convertControls() {

        Set<Integer> keySet = offsetSave.keySet();
        PartitionCodeAndOffSet.clear();

        for (Integer partitioncode : keySet) {

            long offset = offsetSave.get(partitioncode);


            Map<String, Long> jsonObject = new HashMap<>();
            jsonObject.put("patition_code", Long.valueOf(partitioncode));
            jsonObject.put("offset", offset + 1);
            PartitionCodeAndOffSet.add(jsonObject);
        }
    }

    public void setPartitionCodeAndOffSet(int partitioncode, long offset) {

        offsetSave.put(partitioncode, offset);

    }


    public List<DataLakeStreamData> load() throws Exception {

        if (TableName == null || "".equals(TableName)) {
            throw new Exception("TableName 为 null");
        }

        if (readCount == 0) {
            throw new Exception("readCount 为 0");
        }


        convertControls();

        // 获取输入输出流

        Map<String, Object> stream_read = new HashMap<>();
        stream_read.put("table_name", TableName);
        stream_read.put("read_count", readCount);
        stream_read.put("patition_mess", PartitionCodeAndOffSet);

        Map<String, Map<String, Object>> jsonObject = new HashMap<>();
        jsonObject.put("stream_read", stream_read);

        String dataLakeMessage = new Gson().toJson(jsonObject);

        byte[] bytes = dataLakeMessage.getBytes();

        int bytes_len = bytes.length;

        out.writeInt(bytes_len);
        out.write(bytes);

        List<DataLakeStreamData> resList = new ArrayList<DataLakeStreamData>();

        while (true) {
            int mess_len = in.readInt();

            if (mess_len == -1) {
                break;
            } else if (mess_len == -2) {
                int len = in.readInt();
                byte[] mess = new byte[len];
                in.readFully(mess);

                throw new Exception(new String(mess));

            } else {
                byte[] mess = new byte[mess_len];
                in.readFully(mess);
                byte[] uncompressedData = Snappy.uncompress(mess);
                List<DataLakeStreamData> dataLakeStreamDataList = gson.fromJson(new String(uncompressedData), new TypeToken<List<DataLakeStreamData>>() {
                }.getType());
                resList.addAll(dataLakeStreamDataList);
            }
        }

        return resList;
    }

    public void close() throws IOException {
        socket.close();
        out.close();
        in.close();
    }


}
