package org.activiti.engine.impl.interceptor;

import java.util.HashMap;
import java.util.Map;

public class RequestHolder {
    public  static Map<String,String> udalConfMap = new HashMap<>();
    private final static ThreadLocal<String> requestHolder = new ThreadLocal<>();
    // 用来传递一些中间变量
    private final static ThreadLocal<byte[]> bytes = new ThreadLocal<>();

    public static void addbytes(byte[] value) {
        bytes.set(value);
    }

    public static byte[] getbytes() {
        return bytes.get();
    }

    public static void removebytes() {
        bytes.remove();
    }
    public static void add(String id) {
        requestHolder.set(id);
    }

    public static String getId() {
        return requestHolder.get();
    }

    public static void remove() {
        requestHolder.remove();
    }
}
