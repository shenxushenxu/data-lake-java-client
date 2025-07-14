package com.shenxu.cn.deserializer;

import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
import com.shenxu.cn.entity.DataLakeStreamData;
import com.shenxu.cn.entity.LineData;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

public class DataLakeStreamDataDeserializer implements JsonDeserializer<DataLakeStreamData> {
    @Override
    public DataLakeStreamData deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {


        if (jsonElement.isJsonObject()) {
            JsonObject obj = jsonElement.getAsJsonObject();
            String tableName = obj.get("table_name").getAsString();
            String majorValue = obj.get("major_value").getAsString();
            String crudType = obj.get("_crud_type").getAsString();
            int partitionCode = obj.get("partition_code").getAsInt();
            long offset = obj.get("offset").getAsLong();
            JsonObject jsonObject  = obj.get("data").getAsJsonObject();

            Map<String, Object> map = new HashMap<>();
            for (String key : jsonObject.keySet()){
                String value = jsonObject.get(key).getAsString();
                map.put(key, value);
            }


            LineData data = new LineData();
            data.setMap(map);

            DataLakeStreamData dataLakeStreamData = new DataLakeStreamData(
                    tableName,
                    majorValue,
                    crudType,
                    partitionCode,
                    offset,
                    data
            );
            return dataLakeStreamData;
        }else {
            throw new JsonParseException("DataLakeStreamDataDeserializer 序列化失败");
        }



    }
}
