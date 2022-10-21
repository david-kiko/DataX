package com.alibaba.datax.dataxservice.netty.client;

import com.alibaba.datax.dataxservice.netty.model.MessageTransportVo;
import io.netty.channel.Channel;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ClientChannelHandlerPool {
  private ClientChannelHandlerPool() {
  }

  private static Channel channel = null;
  protected static final Map<String, ConcurrentLinkedQueue<MessageTransportVo>> CHANNEL_MSG =
      new ConcurrentHashMap<>();

  public static Channel getChannel() {
    return channel;
  }

  public static void setChannel(Channel channel) {
    ClientChannelHandlerPool.channel = channel;
  }
}
