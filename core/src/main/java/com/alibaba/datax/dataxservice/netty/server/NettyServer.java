package com.alibaba.datax.dataxservice.netty.server;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.dataxservice.netty.enumeration.NettyErrorCode;
import com.alibaba.datax.dataxservice.netty.handler.JsonMessageDecoder;
import com.alibaba.datax.dataxservice.netty.handler.JsonMessageEncoder;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.json.JsonObjectDecoder;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PreDestroy;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class NettyServer implements Callable<Integer> {
  private static final Logger LOG = LoggerFactory.getLogger(NettyServer.class);
  private NioEventLoopGroup bossGroup = new NioEventLoopGroup();
  private NioEventLoopGroup workerGroup = new NioEventLoopGroup();
  private int port;

  public NettyServer(int port) {
    this.port = port;
  }

  @Override
  public Integer call() throws InterruptedException {
    LOG.info("Netty server is starting");
    ServerBootstrap serverBootstrap = new ServerBootstrap();
    serverBootstrap
        .group(bossGroup, workerGroup)

        // 指定Channel
        .channel(NioServerSocketChannel.class)

        //服务端可连接队列数,对应TCP/IP协议listen函数中backlog参数
        .option(ChannelOption.SO_BACKLOG, 1024)

        //设置TCP长连接,一般如果两个小时内没有数据的通信时,TCP会自动发送一个活动探测数据报文
        .childOption(ChannelOption.SO_KEEPALIVE, Boolean.TRUE)

        //将小的数据包包装成更大的帧进行传送，提高网络的负载
        .childOption(ChannelOption.TCP_NODELAY, Boolean.TRUE)

        .childHandler(new ChannelInitializer<NioSocketChannel>() {
          @Override
          protected void initChannel(NioSocketChannel ch) {
            ch.pipeline()
                // 添加消息发送/接收编码解析器
                .addLast(new JsonObjectDecoder())
                .addLast(new JsonMessageDecoder())
                .addLast(new JsonMessageEncoder())
                // 添加心跳机制处理器，300秒未读取到消息触发NettyServerHandler的userEventTriggered方法
                .addLast(new IdleStateHandler(300, 0, 0, TimeUnit.SECONDS))
                // 处理器，处理消息的核心方法全在里面，可以通过override来处理消息
                .addLast(new NettyServerHandler());
          }
        });
    serverBootstrap.bind(port).sync();
    LOG.info("Netty server is started, port:{}", port);
    Runtime.getRuntime().addShutdownHook(new Thread(){
      @Override
      public void run() {
        try {
          bossGroup.shutdownGracefully().sync();
          workerGroup.shutdownGracefully().sync();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw DataXException.asDataXException(NettyErrorCode.CHANNEL_CLOSE_ERROR, "消息通道关闭异常", e);
        }
      }
    });
    return port;
  }

  @PreDestroy
  public void destroy() throws InterruptedException {
    bossGroup.shutdownGracefully().sync();
    workerGroup.shutdownGracefully().sync();
  }
}
