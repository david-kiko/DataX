package com.alibaba.datax.dataxservice.netty.server;

import com.alibaba.datax.dataxservice.netty.model.MessageTransportVo;
import io.netty.channel.Channel;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArraySet;

public class ServerChannelHandlerPool {
  // 存放Channel通道的静态缓存集合，channel用来给client发消息使用
  public static final Map<Integer, Channel> CHANNEL_MAP = new ConcurrentHashMap<>();
  // 存放消息的静态缓存集合，可以供取出消息消费，暂时用不到
  public static final Map<Integer, ConcurrentLinkedQueue<MessageTransportVo>> CHANNEL_MSG =
      new ConcurrentHashMap<>();
  // 存放连接过的taskGroupId用于判断driver主任务是否可以往下走
  public static  final Set<Integer> TASK_GROUP_ID_SET = new CopyOnWriteArraySet<>();
}
