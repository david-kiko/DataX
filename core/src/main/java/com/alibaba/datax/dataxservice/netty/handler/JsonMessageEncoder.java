package com.alibaba.datax.dataxservice.netty.handler;

import com.alibaba.datax.dataxservice.netty.model.MessageTransportVo;
import com.alibaba.fastjson.JSON;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * 编码解析处理器，将json格式的消息封装成vo类
 */
public class JsonMessageEncoder extends MessageToByteEncoder<MessageTransportVo> {
  @Override
  protected void encode(ChannelHandlerContext channelHandlerContext,
                        MessageTransportVo messageTransportVo, ByteBuf byteBuf) {
    byte[] bytes = JSON.toJSONBytes(messageTransportVo);
    byteBuf.writeBytes(bytes);
  }
}
