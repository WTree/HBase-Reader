package com.common.data.hbase.read.cache;


import com.common.data.hbase.read.EnumPack;

import java.lang.reflect.Field;

/**
 * @author wtree
 */
public class FieldCacheInfo {

    private String HBaseKey;
    private String typeName;
    private String fieldName;

    private Field field;
    private EnumPack packType;

    private String getName;

    private String family;

    private String tableName;

    public Field getField() {
        return field;
    }


    public String getGetName() {
        return getName;
    }

    public void setGetName(String getName) {
        this.getName = getName;
    }

    public void setField(Field field) {
        this.field = field;
    }

    public String getHBaseKey() {
        return HBaseKey;
    }

    public void setHBaseKey(String HBaseKey) {
        this.HBaseKey = HBaseKey;
    }

    public String getTypeName() {
        return typeName;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public EnumPack getPackType() {
        return packType;
    }

    public void setPackType(EnumPack packType) {
        this.packType = packType;
    }

    public String getFamily() {
        return family;
    }

    public void setFamily(String family) {
        this.family = family;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
}
