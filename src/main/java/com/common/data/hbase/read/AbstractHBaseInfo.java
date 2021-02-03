package com.common.data.hbase.read;


import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * @author wtree
 */
public abstract class AbstractHBaseInfo {

    @JsonIgnore
    private byte[] row;

    /**
     * 优先获取这里的字段
     */
    @JsonIgnore
    private String tableName;

    public byte[] getRow() {
        return row;
    }

    public void setRow(byte[] row) {
        this.row = row;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
}
