package com.common.data.hbase.read;

import com.common.data.hbase.read.cache.FieldCacheInfo;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.*;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Get;

/**
 * @author wtree
 */
public class BaseHBaseReader {
    //缓存1000个实体类
    private static final int MAX_CACHE_SIZE = 100;
    private static final float DEFAULT_LOAD_FACTOR = 0.75f;
    static int capacity = (int) Math.ceil(MAX_CACHE_SIZE / DEFAULT_LOAD_FACTOR) + 1;

    static Map<String, Map<String, FieldCacheInfo>> mCacheMap = Collections.synchronizedMap(new
            LinkedHashMap<>(capacity, DEFAULT_LOAD_FACTOR, true));


    static Map<String, String> readGetMethod(Class<?> cls) {

        Map<String, String> map = new HashMap<>(10);
        Method[] methods = cls.getMethods();
        for (Method method : methods) {
            method.setAccessible(true);

            String name = method.getName();


            JsonProperty property = method.getAnnotation(JsonProperty.class);
            if (property == null) {
                if (name.startsWith("get")) {
                    String value = name.replace("get", "");
                    map.put(value.toLowerCase(), value);
                }

            } else {
                if (name.startsWith("get")) {
                    String value = name.replace("get", "");
                    map.put(value.toLowerCase(), property.value());
                }

            }


            method.setAccessible(false);
        }

        return map;

    }


    public static Map<String, String> readGetJsonProperty(Class<?> cls) {

        Map<String, String> map = new HashMap<>(10);
        Method[] methods = cls.getMethods();
        for (Method method : methods) {
            method.setAccessible(true);

            String name = method.getName();


            JsonProperty property = method.getAnnotation(JsonProperty.class);
            if (property != null) {
                if (name.startsWith("get")) {
                    String value = name.replace("get", "");
                    map.put(value.toLowerCase(), property.value());
                }
            }


            method.setAccessible(false);
        }

        return map;

    }

    public static Map<String, FieldCacheInfo> readAndParse(Class<?> cls) {

        String key = cls.getName();
        Map<String, FieldCacheInfo> cacheInfo = mCacheMap.get(key);
        if (cacheInfo == null) {
            Field[] fields = cls.getDeclaredFields();
            cacheInfo = new HashMap<>(fields.length);

            Map<String, String> getMap = readGetJsonProperty(cls);
            for (Field field : fields) {

                field.setAccessible(true);
                HBaseFieldName fieldName = field.getAnnotation(HBaseFieldName.class);
                if (fieldName == null) {
                    continue;
                }
                Type type = field.getGenericType();
                String fieldKey = fieldName.HBaseKey();
                FieldCacheInfo fieldCacheInfo = new FieldCacheInfo();
                fieldCacheInfo.setFieldName(field.getName());
                fieldCacheInfo.setHBaseKey(fieldKey);
                fieldCacheInfo.setField(field);
                fieldCacheInfo.setFamily(fieldName.family());
                fieldCacheInfo.setPackType(fieldName.PackType());
                fieldCacheInfo.setTypeName(type.getTypeName());
                String getName = getMap.get(field.getName().toLowerCase());
                if (StringUtils.isEmpty(getName)) {
                    fieldCacheInfo.setGetName(field.getName());
                } else {
                    fieldCacheInfo.setGetName(getName);
                }


                cacheInfo.put(fieldKey, fieldCacheInfo);

            }
            mCacheMap.put(key, cacheInfo);
        }
        return cacheInfo;
    }


    /**
     * 严苛模式，如果某个字段获取失败，就返回null,但是实现了 IFieldCheck 除外
     *
     * @param cls
     * @param result
     * @param cacheInfoMap
     * @param needReadField
     * @param <T>
     * @return
     * @throws Exception
     */
    static <T> T resultToInstance(Class<?> cls, Result result, Map<String, FieldCacheInfo> cacheInfoMap, String[] needReadField) throws Exception {

        MsgPackReader reader = MsgPackReader.create();
        T t = (T) cls.newInstance();
        if (t instanceof AbstractHBaseInfo) {
            ((AbstractHBaseInfo) t).setRow(result.getRow());
        }

        boolean isExitNone = false;
        for (String key : cacheInfoMap.keySet()) {
            FieldCacheInfo info = cacheInfoMap.get(key);
            if (info == null) {
                isExitNone = true;
                continue;
            }
            if (!isNeedRead(needReadField, info.getFieldName())) {
                continue;
            }
            byte[] data = result.getValue(info.getFamily().getBytes(), info.getHBaseKey().getBytes());
            if (data == null) {
                isExitNone = true;
                continue;
            }
            Field field = info.getField();
            field.setAccessible(true);

            Object value;
            if (info.getPackType() == EnumPack.PACK_MSG) {
                value = parsePack(info.getTypeName(), data, reader);
            } else {
                value = parseBytes(info.getTypeName(), data);
            }
            field.set(t, value);
            field.setAccessible(false);
        }

        if (isExitNone) {
            if (t instanceof IFieldCheck) {
                boolean isEmpty = ((IFieldCheck) t).isEmptyValue();
                if (isEmpty) {
                    return null;
                }
            } else {
                return null;
            }
        }

        return t;
    }


    static Object parseBytes(String typeName, byte[] bytes) {
        if (typeName.equalsIgnoreCase(double.class.getTypeName())) {
            return Bytes.toDouble(bytes);
        } else if (typeName.equalsIgnoreCase(int.class.getTypeName())) {
            return Bytes.toInt(bytes);
        } else if (typeName.equalsIgnoreCase(long.class.getTypeName())) {
            return Bytes.toLong(bytes);
        } else if (typeName.equalsIgnoreCase(float.class.getTypeName())) {
            return Bytes.toFloat(bytes);
        } else if (typeName.equalsIgnoreCase(String.class.getTypeName())) {
            return Bytes.toString(bytes);
        } else if (typeName.equalsIgnoreCase(BigDecimal.class.getTypeName())) {
            return Bytes.toBigDecimal(bytes);
        } else if (typeName.equalsIgnoreCase(short.class.getTypeName())) {
            return Bytes.toShort(bytes);
        } else if (typeName.equalsIgnoreCase(boolean.class.getTypeName())) {
            return Bytes.toBoolean(bytes);
        } else {
            return Bytes.toString(bytes);
        }
    }

    static Object parsePack(String typeName, byte[] bytes, MsgPackReader reader) throws IOException {
        if (typeName.equalsIgnoreCase(double.class.getTypeName())) {
            return reader.getDouble(bytes);
        } else if (typeName.equalsIgnoreCase(int.class.getTypeName())) {
            return reader.getInt(bytes);
        } else if (typeName.equalsIgnoreCase(long.class.getTypeName())) {
            return reader.getLong(bytes);
        } else if (typeName.equalsIgnoreCase(float.class.getTypeName())) {
            return reader.getFloat(bytes);
        } else if (typeName.equalsIgnoreCase(String.class.getTypeName())) {
            return reader.getString(bytes);
        } else if (typeName.equalsIgnoreCase(short.class.getTypeName())) {
            return reader.getShort(bytes);
        } else if (typeName.equalsIgnoreCase(boolean.class.getTypeName())) {
            return reader.getBoolean(bytes);
        } else {
            return reader.getString(bytes);
        }
    }


    /**
     * 先解析简单的类型
     *
     * @param type
     * @param bytes
     * @return
     */
    static Object parseBytes(Type type, byte[] bytes) {
        if (type.getTypeName().equalsIgnoreCase(double.class.getTypeName())) {
            return Bytes.toDouble(bytes);
        } else if (type.getTypeName().equalsIgnoreCase(int.class.getTypeName())) {
            return Bytes.toInt(bytes);
        } else if (type.getTypeName().equalsIgnoreCase(long.class.getTypeName())) {
            return Bytes.toLong(bytes);
        } else if (type.getTypeName().equalsIgnoreCase(float.class.getTypeName())) {
            return Bytes.toFloat(bytes);
        } else if (type.getTypeName().equalsIgnoreCase(String.class.getTypeName())) {
            return Bytes.toString(bytes);
        } else if (type.getTypeName().equalsIgnoreCase(BigDecimal.class.getTypeName())) {
            return Bytes.toBigDecimal(bytes);
        } else if (type.getTypeName().equalsIgnoreCase(short.class.getTypeName())) {
            return Bytes.toShort(bytes);
        } else if (type.getTypeName().equalsIgnoreCase(boolean.class.getTypeName())) {
            return Bytes.toBoolean(bytes);
        } else {
            return Bytes.toString(bytes);
        }
    }

    static Object parsePack(Type type, byte[] bytes, MsgPackReader reader) throws IOException {
        if (type.getTypeName().equalsIgnoreCase(double.class.getTypeName())) {
            return reader.getDouble(bytes);
        } else if (type.getTypeName().equalsIgnoreCase(int.class.getTypeName())) {
            return reader.getInt(bytes);
        } else if (type.getTypeName().equalsIgnoreCase(long.class.getTypeName())) {
            return reader.getLong(bytes);
        } else if (type.getTypeName().equalsIgnoreCase(float.class.getTypeName())) {
            return reader.getFloat(bytes);
        } else if (type.getTypeName().equalsIgnoreCase(String.class.getTypeName())) {
            return reader.getString(bytes);
        } else if (type.getTypeName().equalsIgnoreCase(short.class.getTypeName())) {
            return reader.getShort(bytes);
        } else if (type.getTypeName().equalsIgnoreCase(boolean.class.getTypeName())) {
            return reader.getBoolean(bytes);
        } else {
            return reader.getString(bytes);
        }
    }


    static Get buildGet(byte[] rowKey, byte[] family, List<byte[]> columns) {
        Get get = new Get(rowKey);
        if (family != null && columns != null) {
            int var5 = columns.size();
            for (int var6 = 0; var6 < var5; ++var6) {
                byte[] column = columns.get(var6);
                get.addColumn(family, column);
            }
        }

        return get;
    }


    public static boolean isNeedRead(String[] fields, String field) {
        if (fields == null) {
            return true;
        }
        for (String name : fields) {
            if (name.equals(field)) {
                return true;
            }
        }
        return false;
    }

}
