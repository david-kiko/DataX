package com.alibaba.datax.dataxservice.netty.server;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.communication.LocalTGCommunicationManager;
import com.alibaba.datax.core.taskgroup.TaskGroupInfo;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.dataxservice.netty.enumeration.MessageTypeEnum;
import com.alibaba.datax.dataxservice.netty.model.MessageTransportVo;
import com.alibaba.datax.dataxservice.netty.util.SendMessageUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentLinkedQueue;

public class NettyServerHandler extends ChannelInboundHandlerAdapter {
  private static final Logger LOG = LoggerFactory.getLogger(NettyServerHandler.class);

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) {
    // 1. 获取数据
    MessageTransportVo vo = (MessageTransportVo) msg;
    if (vo.getType() == MessageTypeEnum.CONNECT_INIT.getType()) {
      // 初始化逻辑，client第一次连接会发送一次这个消息，趁机将taskGroupId和channel绑定起来
      int taskGroupId = vo.getTaskGroupId();
      String taskGroupString = TaskGroupInfo.taskGroupMap.get(taskGroupId);

      vo.setConfiguration(taskGroupString.replace(CoreConstant.DATAX_HOME, "DATAX_HOME"));
      LOG.info("taskGroupId:{} is bind channel", taskGroupId);
      ServerChannelHandlerPool.CHANNEL_MAP.put(vo.getTaskGroupId(), ctx.channel());
      ServerChannelHandlerPool.CHANNEL_MSG.put(vo.getTaskGroupId(), new ConcurrentLinkedQueue<>());
      ServerChannelHandlerPool.TASK_GROUP_ID_SET.add(vo.getTaskGroupId());
      // 第一次连接时，将配置信息发给客户端
      ctx.channel().writeAndFlush(vo);
    } else if (vo.getType() == MessageTypeEnum.UPDATE_COMMUNICATION.getType()) {
      // 核心逻辑，获取到taskGroup传递过来的日志类，将其更新到本地，待job使用归纳
      LocalTGCommunicationManager.updateTaskGroupCommunication(vo.getTaskGroupId(),
          vo.getCommunication());
    } else if (vo.getType() == MessageTypeEnum.MESSAGE_RESPONSE.getType()) {
      // 接受到【已收到】消息，将他放到集合中待util处理，直接return不再走下一步
      SendMessageUtil.CHANNEL_MSG_RETURN.add(vo.getId());
      return;
    } else if(vo.getType() == MessageTypeEnum.CONNECT_HEARTBEAT.getType()){
      // 心跳探测，一般不需要做动作，返回MESSAGE_RESPONSE消息就行
      if (ServerChannelHandlerPool.CHANNEL_MAP.get(vo.getTaskGroupId()) == null){
        ServerChannelHandlerPool.CHANNEL_MAP.put(vo.getTaskGroupId(), ctx.channel());
      }
      if (ServerChannelHandlerPool.CHANNEL_MSG.get(vo.getTaskGroupId()) == null){
        ServerChannelHandlerPool.CHANNEL_MSG.put(vo.getTaskGroupId(),new ConcurrentLinkedQueue<>());
      }
    }
      // 收集其他消息，其实没啥用，client也没有传递什么其他消息的逻辑，留着以后扩展
      // 注释掉无用代码【ServerChannelHandlerPool.CHANNEL_MSG.get(vo.getTaskGroupId()).add(vo);】

    if (ctx.channel().isActive()){
      // 最后一步，没有直接return的消息都回写一个【已收到】的消息
      MessageTransportVo resVo = new MessageTransportVo();
      resVo.setType(MessageTypeEnum.MESSAGE_RESPONSE.getType());
      resVo.setId(vo.getId());
      ctx.channel().writeAndFlush(resVo);
    }

    if (!MessageTypeEnum.typeExistsInServer(vo.getType())){
      LOG.error("Netty Server 收到未知消息：{}", vo);
    }

  }

  @Override
  public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
    // 心跳检测，200秒没有读取到数据，则自动关闭连接,并移除CHANNEL_MAP集合
    if (evt instanceof IdleStateEvent) {
      IdleStateEvent event = (IdleStateEvent) evt;
      if (IdleState.READER_IDLE == event.state()) {
        ctx.channel().close();
        ServerChannelHandlerPool.CHANNEL_MAP.entrySet().removeIf(entry -> !entry.getValue().isActive());
      }
    }
    super.userEventTriggered(ctx, evt);
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    // 连接关闭
    ctx.channel().close();
    ServerChannelHandlerPool.CHANNEL_MAP.entrySet().removeIf(entry -> !entry.getValue().isActive());
    super.channelInactive(ctx);
  }
}
