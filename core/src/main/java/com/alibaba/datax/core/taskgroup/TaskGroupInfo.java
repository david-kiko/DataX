package com.alibaba.datax.core.taskgroup;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSONObject;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TaskGroupInfo {
    public static Map<Integer, String> taskGroupMap = new ConcurrentHashMap<>();
}
