package com.common.data.hbase.read;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;

import java.io.IOException;

/**
 * hbase的辅助工具类，创建Scan，读取Result
 * Created by yangsm on 14-6-20.
 */
class HBaseUtil {

    public static Get buildGet(byte[] rowKey, byte[] family, byte[]... columns) {
        Get get = new Get(rowKey);
        if (family != null && columns != null) {
            for (byte[] column : columns) {
                get.addColumn(family, column);
            }
        }
        return get;
    }

    public static Scan buildScan(
            byte[] startRow,
            byte[] stopRow
    ) {
        return buildScan(startRow, stopRow, null, null);
    }

    public static Scan buildScan(
            byte[] startRow,
            byte[] stopRow,
            Filter filter
    ) {
        return buildScan(startRow, stopRow, filter, null);
    }


    public static Scan buildScan(
            byte[] startRow,
            byte[] stopRow,
            Filter filter,
            byte[] family,
            byte[]... columns
    ) {
        Scan scan = buildScan();

        if (startRow != null) {
            scan.withStartRow(startRow);
        }
        if (stopRow != null) {
            scan.withStopRow(stopRow);
        }

        if (filter != null) {
            scan.setFilter(filter);
        }

        if (family != null && columns != null) {
            for (byte[] column : columns) {
                scan.addColumn(family, column);
            }
        }


        return scan;
    }

    public static Scan buildScan() {
        Scan scan = new Scan();
        scan.setCaching(1000);

        return scan;
    }

    public static Scan buildBatchScan(){
        Scan scan = new Scan();
        scan.setCaching(1000);
        scan.setCacheBlocks(false);
        return scan;
    }

    public static byte[] getBytes(Result result, byte[] family, byte[] column) {
        if (result == null || family == null || column == null) {
            return null;
        } else {
            return result.getValue(family, column);
        }
    }

    public static <T> T getValue(Result result, byte[] family, byte[] column, BytesToValue<T> reader) throws IOException {
        return getValue(result, family, column, reader, null);
    }

    public static <T> T getValue(Result result, byte[] family, byte[] column, BytesToValue<T> reader, T defaultValue) throws IOException {
        byte[] bytes = getBytes(result, family, column);
        if (bytes == null) {
            return defaultValue;
        } else {
            return reader.read(bytes);
        }
    }

    public static byte[] getBytes(Table table, byte[] rowKey, byte[] family, byte[] column) throws IOException {
        if (table == null || rowKey == null || family == null || column == null) {
            return null;
        } else {
            Get get = new Get(rowKey);
            get.addColumn(family, column);
            Result result = table.get(get);
            return result.getValue(family, column);
        }
    }

    public static <T> T getValue(Table table, byte[] rowKey, byte[] family, byte[] column, BytesToValue<T> reader) throws IOException {
        return getValue(table, rowKey, family, column, reader, null);
    }

    public static <T> T getValue(Table table, byte[] rowKey, byte[] family, byte[] column, BytesToValue<T> reader, T defaultValue) throws IOException {
        byte[] bytes = getBytes(table, rowKey, family, column);
        if (bytes == null) {
            return defaultValue;
        } else {
            return reader.read(bytes);
        }
    }

    @FunctionalInterface
    public interface BytesToValue<T> {

        T read(byte[] bytes) throws IOException;

    }
}
