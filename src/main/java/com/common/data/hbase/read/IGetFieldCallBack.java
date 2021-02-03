package com.common.data.hbase.read;


import com.common.data.hbase.read.interceptor.BeanFactoryInterceptorHelper;

/**
 * 
 * @author wtree
 * @see BeanFactoryInterceptorHelper#intercept(Object, String[])
 *
 * @see IFieldCheck 有实现这个的字段必须包含在这里
 */
public interface IGetFieldCallBack {

    String[] fieldValue();
}
