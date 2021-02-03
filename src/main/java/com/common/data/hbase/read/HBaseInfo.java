package com.common.data.hbase.read;

import java.lang.annotation.*;

/**
 * @author wtree
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.TYPE})
@Documented
public @interface HBaseInfo {


    String tableName();

    String tableNameSpace();

}
