package com.common.data.hbase.read;


import com.common.data.hbase.read.cache.FieldCacheInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * * @author wtree
 */
public class HBaseReadHelper extends BaseHBaseReader {

    public static <T> List<T> scan(Connection connection, Class<?> cls, byte[] rowPrefix) throws Exception {

        return scan(connection, cls, rowPrefix, -1, null);
    }

    public static <T> List<T> scan(Connection connection, Class<?> cls, byte[] starRow, byte[] stopRow) throws Exception {

        return scan(connection, cls, starRow, stopRow, false, -1, null);
    }

    public static <T> List<T> scan(Connection connection, Class<?> cls, byte[] rowPrefix, int limit) throws Exception {
        return scan(connection, cls, rowPrefix, limit, null);
    }

    public static <T> List<T> scan(Connection connection, Class<?> cls, byte[] rowPrefix, int limit, IGetFieldCallBack callBack) throws Exception {

        return scan(connection, null, cls, rowPrefix, limit, callBack);
    }

    public static <T> List<T> scan(Connection connection, String heightTableName, Class<?> cls, byte[] rowPrefix, int limit, IGetFieldCallBack callBack) throws Exception {
        HBaseInfo annotation = cls.getAnnotation(HBaseInfo.class);
        if (annotation == null) {
            throw new RuntimeException("该:" + cls.getName() + "没有使用@HBaseInfo注解");
        }

        String name = annotation.tableName();
        String nameSpace = annotation.tableNameSpace();
        TableName tableName = getTableName(name, heightTableName, nameSpace);

        return scanHBaseImpl(cls, connection, tableName, rowPrefix, limit, callBack);
    }

    public static <T> List<T> scan(Connection connection, Class<?> cls, byte[] starRow, byte[] stopRow, boolean isReserve, int limit) throws Exception {

        return scan(connection, cls, starRow, stopRow, isReserve, limit, null);
    }

    public static <T> List<T> scan(Connection connection, Class<?> cls, byte[] starRow, byte[] stopRow, boolean isReserve, int limit, IGetFieldCallBack callBack) throws Exception {

        return scan(connection, null, cls, starRow, stopRow, isReserve, limit, callBack);
    }

    public static <T> List<T> scan(Connection connection, String heightTableName, Class<?> cls, byte[] starRow, byte[] stopRow, boolean isReserve, int limit, IGetFieldCallBack callBack) throws Exception {
        HBaseInfo annotation = cls.getAnnotation(HBaseInfo.class);
        if (annotation == null) {
            throw new RuntimeException("该:" + cls.getName() + "没有使用@HBaseInfo注解");
        }

        String name = annotation.tableName();
        String nameSpace = annotation.tableNameSpace();
        TableName tableName = getTableName(name, heightTableName, nameSpace);

        return scanHBaseImpl(cls, connection, tableName, starRow, stopRow, isReserve, limit, callBack);
    }


    public static <T> T read(Connection connection, Class<T> cls, byte[] rowKey) throws Exception {
        return read(connection, cls, null, rowKey);
    }

    public static <T> T read(Connection connection, Class<T> cls, IGetFieldCallBack callBack, byte[] rowKey) throws Exception {

        return read(connection, null, cls, callBack, rowKey);
    }

    public static <T> T read(Connection connection, String heightTableName, Class<T> cls, IGetFieldCallBack callBack, byte[] rowKey) throws Exception {

        HBaseInfo annotation = cls.getAnnotation(HBaseInfo.class);
        if (annotation == null) {
            throw new RuntimeException("该:" + cls.getName() + "没有使用@HBaseInfo注解");
        }

        String name = annotation.tableName();
        String nameSpace = annotation.tableNameSpace();
        TableName tableName = getTableName(name, heightTableName, nameSpace);
        return readHBaseImpl(cls, connection, tableName, callBack, rowKey);
    }

    public static <T> List<T> readList(Connection connection, Class<?> cls, IGetFieldCallBack callBack, List<byte[]> rowKey) throws Exception {
        byte[][] array = rowKey.toArray(new byte[rowKey.size()][]);

        return readList(connection, cls, callBack, array);
    }

    public static <T> List<T> readList(Connection connection, Class<?> cls, IGetFieldCallBack callBack, byte[]... rowKey) throws Exception {


        return readList(connection, null, cls, callBack, rowKey);
    }

    public static <T> List<T> readList(Connection connection, String heightTableName, Class<?> cls, IGetFieldCallBack callBack, byte[]... rowKey) throws Exception {

        if (rowKey == null) {
            return null;
        }
        HBaseInfo annotation = cls.getAnnotation(HBaseInfo.class);
        if (annotation == null) {
            throw new RuntimeException("该:" + cls.getName() + "没有使用@HBaseInfo注解");
        }

        String name = annotation.tableName();
        String nameSpace = annotation.tableNameSpace();
        TableName tableName = getTableName(name, heightTableName, nameSpace);
        return readHBaseImpl(cls, connection, tableName, callBack, rowKey);
    }

    static <T> List<T> scanHBaseImpl(Class<?> cls, Connection connection, TableName tableName, byte[] rowPrefix, int limit, IGetFieldCallBack callBack)
            throws Exception {

        Map<String, FieldCacheInfo> cache = readAndParse(cls);
        String[] needReadField = callBack == null ? null : callBack.fieldValue();

        Scan scan = HBaseUtil.buildScan().setCacheBlocks(false);

        Iterator<String> itM = cache.keySet().iterator();
        while (itM.hasNext()) {
            String key = itM.next();
            FieldCacheInfo cacheInfo = cache.get(key);
            if (!isNeedRead(needReadField, cacheInfo.getFieldName())) {
                continue;
            }
            scan.addColumn(cacheInfo.getFamily().getBytes(), cacheInfo.getHBaseKey().getBytes());
        }

        if (limit > 0) {
            scan.setLimit(limit);
        }
        scan.setRowPrefixFilter(rowPrefix);


        List<T> data = new ArrayList<>(10);
        try (Table table = connection.getTable(tableName);
             ResultScanner scanner = table.getScanner(scan)) {
            Iterator<Result> it = scanner.iterator();
            while (it.hasNext()) {
                Result result = it.next();
                T t = resultToInstance(cls, result, cache, needReadField);
                if (t != null) {
                    data.add(t);
                }
            }
        } finally {
        }
        return data;

    }


    static TableName getTableName(String fieldTableName, String heightTableName, String nameSpace) {

        if (StringUtils.isNotEmpty(heightTableName)) {
            return TableName.valueOf(nameSpace.getBytes(), heightTableName.getBytes());
        } else {
            return TableName.valueOf(nameSpace.getBytes(), fieldTableName.getBytes());
        }

    }


    static <T> List<T> scanHBaseImpl(Class<?> cls, Connection connection, TableName tableName, byte[] starRow, byte[] stopRow,
                                     boolean isReverse, int limit, IGetFieldCallBack callBack)
            throws Exception {

        Map<String, FieldCacheInfo> cache = readAndParse(cls);
        String[] needReadField = callBack == null ? null : callBack.fieldValue();

        Scan scan = HBaseUtil.buildScan()
                .setCacheBlocks(false);

        Iterator<String> itM = cache.keySet().iterator();
        while (itM.hasNext()) {
            String key = itM.next();
            FieldCacheInfo cacheInfo = cache.get(key);
            if (!isNeedRead(needReadField, cacheInfo.getFieldName())) {
                continue;
            }
            scan.addColumn(cacheInfo.getFamily().getBytes(), cacheInfo.getHBaseKey().getBytes());
        }
        if (isReverse) {
            scan.setReversed(true);
            if (limit > 0) {
                scan.setLimit(limit);
            }
            scan.withStartRow(stopRow);
            scan.withStopRow(starRow);
        } else {
            if (limit > 0) {
                scan.setLimit(limit);
            }
            scan.withStartRow(starRow);
            scan.withStopRow(stopRow);
        }


        List<T> data = new ArrayList<>(10);
        try (Table table = connection.getTable(tableName); ResultScanner scanner = table.getScanner(scan)) {
            Iterator<Result> it = scanner.iterator();
            while (it.hasNext()) {
                Result result = it.next();
                T t = resultToInstance(cls, result, cache, needReadField);
                if (t != null) {
                    data.add(t);
                }
            }
        } finally {
        }

        return data;

    }


    static <T> T readHBaseImpl(Class<T> cls, Connection connection, TableName tableName, IGetFieldCallBack callBack, byte[] rowKey)
            throws Exception {

        Map<String, FieldCacheInfo> cache = readAndParse(cls);
        String[] needReadField = callBack == null ? null : callBack.fieldValue();
        List<byte[]> columns = new ArrayList<byte[]>();

        String familyName = "";
        Iterator<String> it = cache.keySet().iterator();
        while (it.hasNext()) {
            String key = it.next();
            FieldCacheInfo cacheInfo = cache.get(key);
            if (!isNeedRead(needReadField, cacheInfo.getFieldName())) {
                continue;
            }
            columns.add(cacheInfo.getHBaseKey().getBytes());
            //这里默认一张表就只有一个family
            familyName = cacheInfo.getFamily();
        }

        if (StringUtils.isEmpty(familyName)) {
            return null;
        }

        Get get = buildGet(rowKey, familyName.getBytes(), columns);
        try (Table table = connection.getTable(tableName)) {
            Result result = table.get(get);
            T t = resultToInstance(cls, result, cache, needReadField);
            return t;
        } finally {
        }

    }

    static <T> List<T> readHBaseImpl(Class<?> cls, Connection connection, TableName tableName, IGetFieldCallBack callBack, byte[]... rowKey)
            throws Exception {


        Map<String, FieldCacheInfo> cache = readAndParse(cls);
        String[] needReadField = callBack == null ? null : callBack.fieldValue();
        List<byte[]> columns = new ArrayList<byte[]>();


        String familyName = "";
        Iterator<String> it = cache.keySet().iterator();
        while (it.hasNext()) {
            String key = it.next();
            FieldCacheInfo cacheInfo = cache.get(key);
            if (!isNeedRead(needReadField, cacheInfo.getFieldName())) {
                continue;
            }
            columns.add(cacheInfo.getHBaseKey().getBytes());

            familyName = cacheInfo.getFamily();
        }

        if (StringUtils.isEmpty(familyName)) {
            return Collections.emptyList();
        }

        final String family = familyName;

        List<Get> gets = Arrays.stream(rowKey).map(key -> buildGet(key, family.getBytes(), columns)).collect(Collectors.toList());
        try (Table table = connection.getTable(tableName)) {
            Result[] resultList = table.get(gets);
            List<T> data = Arrays.stream(resultList).flatMap(result -> {
                try {
                    T t = resultToInstance(cls, result, cache, needReadField);
                    if (t == null) {
                        System.out.println("result is NULL");
                        return Stream.empty();
                    }
                    return Stream.of(t);
                } catch (Exception e) {
                    e.printStackTrace();
                    return Stream.empty();
                }
            }).collect(Collectors.toList());
            return data;
        } finally {
        }

    }

}
