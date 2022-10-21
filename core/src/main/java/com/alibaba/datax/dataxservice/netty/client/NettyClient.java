package com.alibaba.datax.dataxservice.netty.client;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.dataxservice.netty.enumeration.NettyConstant;
import com.alibaba.datax.dataxservice.netty.enumeration.NettyErrorCode;
import com.alibaba.datax.dataxservice.netty.handler.JsonMessageDecoder;
import com.alibaba.datax.dataxservice.netty.handler.JsonMessageEncoder;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.json.JsonObjectDecoder;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.AttributeKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class NettyClient implements Callable<Object> {
  private static final Logger LOG = LoggerFactory.getLogger(NettyClient.class);

  private final String host;
  private final int port;
  private final int taskGroupId;
  private static final int MAX_RETRY = 5;

  public NettyClient(String host, int port, int taskGroupId) {
    this.host = host;
    this.port = port;
    this.taskGroupId = taskGroupId;
  }

  @Override
  public Object call() throws InterruptedException {
    LOG.info("Netty client is starting");
    NioEventLoopGroup workerGroup = new NioEventLoopGroup();
    Bootstrap bootstrap = new Bootstrap();
    bootstrap
        // 1.指定线程模型
        .group(workerGroup)
        // 2.指定 IO 类型为 NIO
        .channel(NioSocketChannel.class)
        .option(ChannelOption.SO_KEEPALIVE, Boolean.TRUE)
        .option(ChannelOption.TCP_NODELAY, Boolean.TRUE)
        .attr(AttributeKey.newInstance(NettyConstant.TASK_GROUP_ID), taskGroupId)
        // 3.IO 处理逻辑
        .handler(new ChannelInitializer<SocketChannel>() {
          @Override
          public void initChannel(SocketChannel ch) {
            ch.pipeline()
                .addLast(new JsonObjectDecoder())
                .addLast(new JsonMessageDecoder())
                .addLast(new JsonMessageEncoder())
                // 添加心跳处理，如果200秒未写入消息，则触发心跳方法userEventTriggered
                .addLast(new IdleStateHandler(0, 200, 0, TimeUnit.SECONDS))
                .addLast(new NettyClientHandler());
          }
        });
    // 4.建立连接
    bootstrap.connect(host, port).addListener(future -> {
      if (future.isSuccess()) {
        LOG.info("Netty connect is success,host:{},port:{}", host, port);
      } else {
        LOG.info("Netty connect is failed，retrying...");
        connect(bootstrap, host, port, MAX_RETRY);
      }
    }).sync();
    return 0;
  }


  /**
   * 用于失败重连
   */
  private static void connect(Bootstrap bootstrap, String host, int port, int retry) {
    bootstrap.connect(host, port).addListener(future -> {
      if (future.isSuccess()) {
        LOG.info("Netty connect is success");
      } else if (retry == 0) {
        LOG.error("Netty connect retry failed，connect end,host:{},port:{}", host, port);
        throw new DataXException(NettyErrorCode.CLIENT_CONNECT_ERROR,
            "客户端连接失败，请检查server或者address是否正确");
      } else {
        // 第几次重连
        int order = (MAX_RETRY - retry) + 1;
        // 本次重连的间隔
        int delay = 1 << order;
        LOG.error("Netty connect retry failed，count：{}", order);
        bootstrap.config().group()
            .schedule(() -> connect(bootstrap, host, port, retry - 1), delay, TimeUnit
                .SECONDS);
      }
    });
  }
}
