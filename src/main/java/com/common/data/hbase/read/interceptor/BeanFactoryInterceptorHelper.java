package com.common.data.hbase.read.interceptor;


import com.common.data.hbase.read.BaseHBaseReader;
import com.common.data.hbase.read.HBaseReadHelper;
import com.common.data.hbase.read.cache.FieldCacheInfo;

import java.lang.reflect.Field;
import java.util.*;

/**
 * v0.1版本支持 get注解
 *
 * @author wtree
 */
public class BeanFactoryInterceptorHelper {



    public static  <T> Object intercept(T t, String[] needReadField) throws Exception {


        Class<?> cls = t.getClass();
        Map<String, FieldCacheInfo> cacheInfoMap = HBaseReadHelper.readAndParse(cls);
        Map<String, Object> map = new HashMap<>(needReadField.length);
        Iterator<String> it = cacheInfoMap.keySet().iterator();
        while (it.hasNext()) {
            String key = it.next();

            FieldCacheInfo cacheInfo = cacheInfoMap.get(key);
            if (cacheInfo == null) {
                continue;
            }

            Field field = cacheInfo.getField();
            field.setAccessible(true);
            Object value = field.get(t);
            if (BaseHBaseReader.isNeedRead(needReadField, cacheInfo.getFieldName())) {
                map.put(cacheInfo.getGetName(), value);
            }
            field.setAccessible(false);

        }

        return map;
    }

    public static  <T> List<Object> intercept(List<T> list, String[] needReadField) throws Exception {

        List<Object> data = new ArrayList<>();
        for (T t : list) {
            data.add(intercept(t, needReadField));
        }
        return data;
    }
}
