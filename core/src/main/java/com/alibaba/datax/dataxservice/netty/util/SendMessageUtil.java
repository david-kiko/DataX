package com.alibaba.datax.dataxservice.netty.util;

import com.alibaba.datax.dataxservice.netty.model.MessageTransportVo;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;

public class SendMessageUtil {
  private static final Logger LOG = LoggerFactory.getLogger(SendMessageUtil.class);

  //静态原子类，给每条发消息都设置一个不同的id用来区分
  private static AtomicInteger messageId = new AtomicInteger(0);

  public static final Set<Integer> CHANNEL_MSG_RETURN = new ConcurrentSkipListSet<Integer>();

  /**
   * 直接发送消息，忽略失败异常
   *
   * @param vo 消息主体
   * @param channel 消息通道
   */
  public static void sendMessage(MessageTransportVo vo, Channel channel) {
    if (channel == null || !channel.isActive()) {
      return ;
    }
    int id = messageId.incrementAndGet();
    vo.setId(id);
    channel.writeAndFlush(vo);
  }

  /**
   * 传递消息两次握手静态方法
   *
   * @param vo 需要传递的消息封装类
   * @param channel 消息通道
   * @return 消息是否成功
   */
  public static boolean sendMessageByHandshakes(MessageTransportVo vo, Channel channel) {
    if (channel == null || !channel.isActive()) {
      return false;
    }
    // 静态原子类自增，避免消息id重复
    int id = messageId.incrementAndGet();
    vo.setId(id);
    channel.writeAndFlush(vo);

    // 等待回写的response消息，最多300秒
    int count = 300;
    while (count > 0) {
      // 如果收到了response消息，会将id放入CHANNEL_MSG_RETURN集合，移除成功代表对面已收到消息
      if (CHANNEL_MSG_RETURN.remove(id)) {
        return true;
      } else {
        if (!channel.isActive()) {
          return false;
        }
        count--;
        try {
          // 每次sleep 1s
          Thread.sleep(1000L);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          LOG.error("Thread.sleep error", e);
        }
      }
    }
    return false;
  }
}
