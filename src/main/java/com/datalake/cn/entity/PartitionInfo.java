package com.datalake.cn.entity;

import java.io.Serializable;

public class PartitionInfo implements Serializable {

    private String address;
    private String info;

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getInfo() {
        return info;
    }

    public void setInfo(String info) {
        this.info = info;
    }


    @Override
    public String toString() {
        return "PartitionInfo{" +
                "address='" + address + '\'' +
                ", info='" + info + '\'' +
                '}';
    }
}
