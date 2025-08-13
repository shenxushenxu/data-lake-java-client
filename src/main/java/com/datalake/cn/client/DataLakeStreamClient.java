package com.datalake.cn.client;


import com.datalake.cn.entity.DataLakeLinkData;
import com.google.gson.Gson;
import com.datalake.cn.bincode.BinCodeDeserialize;
import com.datalake.cn.entity.DataLakeStreamData;
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
    private String tableName;
    public Map<Integer, Long> offsetSave;
    private int readCount;

    public DataLakeStreamClient(String serverAddress, int serverPort, String tableName) throws IOException {
        socket = new Socket(serverAddress, serverPort);
        out = new DataOutputStream(socket.getOutputStream());
        in = new DataInputStream(socket.getInputStream());

        this.PartitionCodeAndOffSet = new ArrayList();
        this.offsetSave = new HashMap<Integer, Long>();
        this.tableName = tableName;
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


    public Map<Integer, Long> getOffsetSave(){
        return this.offsetSave;
    }

    public void setPartitionCodeAndOffSet(int partitioncode, long offset) {

        offsetSave.put(partitioncode, offset);

    }


    public List<DataLakeStreamData> load() throws Exception {

        if (readCount == 0) {
            throw new Exception("readCount 为 0");
        }


        convertControls();

        // 获取输入输出流

        Map<String, Object> stream_read = new HashMap<>();
        stream_read.put("table_name", tableName);
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

                List<DataLakeStreamData> dataLakeStreamDataList = this.deserialize(uncompressedData);


                for (DataLakeStreamData dataLakeStreamData: dataLakeStreamDataList){
                    Integer partitionCode = dataLakeStreamData.getPartitionCode();
                    Long offset = dataLakeStreamData.getOffset();
                    offsetSave.put(partitionCode, offset);
                    resList.add(dataLakeStreamData);
                }
            }
        }

        return resList;
    }


    private List<DataLakeStreamData> deserialize(byte[] bytes){
        BinCodeDeserialize binCodeDeserialize = new BinCodeDeserialize(bytes);

        int listSize = binCodeDeserialize.getInt();
        List<DataLakeStreamData> result = new ArrayList<>(listSize);

        for (int i = 0; i < listSize; i++) {
            result.add(parseDataLakeStreamData(binCodeDeserialize));
        }

        return result;
    }

    private static DataLakeStreamData parseDataLakeStreamData(BinCodeDeserialize binCodeDeserialize) {
        // 按字段顺序解析
        String tableName = binCodeDeserialize.getString();
        String majorValue = binCodeDeserialize.getString();
        Map<String, Object> dataMap = binCodeDeserialize.getHashMap();
        String crudType = binCodeDeserialize.getString();
        int partitionCode = binCodeDeserialize.getInt();
        long offset = binCodeDeserialize.getLong(); // i64

        DataLakeLinkData dataLakeLinkData = new DataLakeLinkData();
        dataLakeLinkData.setMap(dataMap);


        return new DataLakeStreamData(
                tableName,
                majorValue,
                crudType,
                partitionCode,
                offset,
                dataLakeLinkData
        );
    }




    public void close() throws IOException {
        socket.close();
        out.close();
        in.close();
    }


}
