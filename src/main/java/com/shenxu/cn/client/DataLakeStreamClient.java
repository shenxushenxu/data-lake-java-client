package com.shenxu.cn.client;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.shenxu.cn.entity.DataLakeStreamData;
import com.shenxu.cn.entity.LineData;
import org.xerial.snappy.Snappy;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.*;

public class DataLakeStreamClient {

    private Socket socket;
    private DataOutputStream out;
    private DataInputStream in;
    private JSONArray PartitionCodeAndOffSet;
    private String TableName;
    public Map<Integer, Long> offsetSave;

    private int readCount;

    public DataLakeStreamClient(String serverAddress, int serverPort) throws IOException {
        socket = new Socket(serverAddress, serverPort);
        out = new DataOutputStream(socket.getOutputStream());
        in = new DataInputStream(socket.getInputStream());

        this.PartitionCodeAndOffSet = new JSONArray();
        this.offsetSave = new HashMap<Integer, Long>();
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


            JSONObject jsonObject = new JSONObject();
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

        JSONObject stream_read = new JSONObject();
        stream_read.put("table_name", TableName);
        stream_read.put("read_count", readCount);
        stream_read.put("patition_mess", PartitionCodeAndOffSet);

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("stream_read", stream_read);
        String ccc = jsonObject.toJSONString();

        System.out.println(ccc);
        byte[] bytes = ccc.getBytes();

        int bytes_len = bytes.length;

        out.writeInt(bytes_len);
        out.write(bytes);

        List<DataLakeStreamData> dataLakeStreamDataList = new ArrayList<DataLakeStreamData>();

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


                JSONArray res_json = JSONArray.parseArray(new String(uncompressedData));


                for (int i = 0; i< res_json.size();i++){
                    JSONObject eachData = res_json.getJSONObject(i);
                    DataLakeStreamData dataLakeStreamData = DataLakeStreamData.parseDataLakeStreamData(eachData);
                    dataLakeStreamDataList.add(dataLakeStreamData);

                    long offset = dataLakeStreamData.getOffset();
                    int patition_code = dataLakeStreamData.getPartitionCode();
                    offsetSave.put(patition_code, offset);
                }
            }
        }

        return dataLakeStreamDataList;
    }

    public void close() throws IOException {
        socket.close();
        out.close();
        in.close();
    }


}
