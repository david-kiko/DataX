package com.alibaba.datax.dataxservice.netty.client;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.core.taskgroup.TaskGroupInfo;
import com.alibaba.datax.dataxservice.netty.enumeration.MessageTypeEnum;
import com.alibaba.datax.dataxservice.netty.enumeration.NettyConstant;
import com.alibaba.datax.dataxservice.netty.enumeration.NettyErrorCode;
import com.alibaba.datax.dataxservice.netty.model.MessageTransportVo;
import com.alibaba.datax.dataxservice.netty.util.SendMessageUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.AttributeKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentLinkedQueue;

public class NettyClientHandler extends ChannelInboundHandlerAdapter {
  private static final Logger LOG = LoggerFactory.getLogger(NettyClientHandler.class);


  @Override
  public void channelActive(ChannelHandlerContext ctx) {
    // 建立一个jvm钩子，程序结束时自动关闭消息通道
    Runtime.getRuntime().addShutdownHook(new Thread(){
      @Override
      public void run() {
        try {
          ctx.channel().close().sync();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw DataXException.asDataXException(NettyErrorCode.CHANNEL_CLOSE_ERROR, "消息通道关闭异常", e);
        }
      }
    });
    // 连接上服务器调用该方法，现将消息channel静态缓存
    ClientChannelHandlerPool.setChannel(ctx.channel());
    // 初始化消息缓存队列
    ClientChannelHandlerPool.CHANNEL_MSG.put(ctx.channel().id().asLongText(),
        new ConcurrentLinkedQueue<>());

    // 初始化数据，发送taskGroupId消息给服务器
    MessageTransportVo vo = new MessageTransportVo();
    vo.setType(MessageTypeEnum.CONNECT_INIT.getType());
    vo.setTaskGroupId(
        (int) ctx.channel().attr(AttributeKey.valueOf(NettyConstant.TASK_GROUP_ID)).get());
    // 写数据
    ctx.channel().writeAndFlush(vo);
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) {
    // 收到消息
    MessageTransportVo vo = (MessageTransportVo) msg;
    LOG.info("MessageTransportVo is " + msg);
    if (vo.getType() == MessageTypeEnum.MESSAGE_RESPONSE.getType()) {
      // 处理 两次握手的response消息
      SendMessageUtil.CHANNEL_MSG_RETURN.add(vo.getId());
      return;
    } else if (vo.getType() == MessageTypeEnum.CONNECT_INIT.getType()) {
      String configurationString = vo.getConfiguration();
      TaskGroupInfo.taskGroupMap.put(vo.getTaskGroupId(), configurationString.replace("DATAX_HOME", "/datax"));
    }
    // 消息放入队列中待使用
//    ClientChannelHandlerPool.CHANNEL_MSG.get(ctx.channel().id().asLongText()).add(vo);


    // 封装【已收到】 response消息告诉服务器client已收到消息
    MessageTransportVo resVo = new MessageTransportVo();
    resVo.setType(MessageTypeEnum.MESSAGE_RESPONSE.getType());
    resVo.setId(vo.getId());
    ctx.channel().writeAndFlush(resVo);

    // 收到服务器发来的强行kill消息，直接结束进程
    if(vo.getType() == MessageTypeEnum.JOB_KILL.getType()){
      System.exit(137);
    }
    if (!MessageTypeEnum.typeExistsInClient(vo.getType())){
      LOG.error("Netty Client 收到未知消息：{}", vo);
    }
  }

  @Override
  public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
    // 心跳检测，如果没有200秒没有给server发送消息，则触发动作
    if (evt instanceof IdleStateEvent) {
      IdleStateEvent event = (IdleStateEvent) evt;
      if (IdleState.WRITER_IDLE == event.state()) {
        // 一般不会走到这一步，taskGroup运行时每隔一段时间都会写log给server，触发了这一步要么是taskGroups工作卡住了，要么就是出错了
        MessageTransportVo vo = new MessageTransportVo();
        vo.setType(MessageTypeEnum.CONNECT_HEARTBEAT.getType());
        vo.setTaskGroupId(
            (int) ctx.channel().attr(AttributeKey.valueOf(NettyConstant.TASK_GROUP_ID)).get());
        boolean isSuccess = SendMessageUtil.sendMessageByHandshakes(vo, ctx.channel());
        if (!isSuccess){
          // 消息发送不成功说明连接断开，直接关闭
          ctx.channel().close();
          ClientChannelHandlerPool.setChannel(null);
        }
      }
    }
    super.userEventTriggered(ctx, evt);
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    // 连接被关闭
    ctx.channel().close();
    ClientChannelHandlerPool.setChannel(null);
    super.channelInactive(ctx);
  }
}
