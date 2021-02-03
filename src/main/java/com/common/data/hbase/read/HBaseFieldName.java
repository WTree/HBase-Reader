package com.common.data.hbase.read;

import java.lang.annotation.*;

/**
 * @author wtree
 * 目前 别名还没有实现，需要注解处理器来生成代码实现
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.TYPE})
@Documented
public @interface HBaseFieldName {

    EnumPack PackType();

    String alias() default "";

    String family() default "r";

    //TODO column  key  byte[]
    String HBaseKey();
}
