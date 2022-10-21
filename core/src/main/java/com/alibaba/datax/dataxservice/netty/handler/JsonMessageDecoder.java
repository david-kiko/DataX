package com.alibaba.datax.dataxservice.netty.handler;

import com.alibaba.datax.dataxservice.netty.model.MessageTransportVo;
import com.alibaba.fastjson.JSON;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * 编码处理器，将消息封装类vo转换成json格式的消息
 */
public class JsonMessageDecoder extends ByteToMessageDecoder {
  private static final Logger LOG = LoggerFactory.getLogger(JsonMessageDecoder.class);

  @Override
  protected void decode(ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf,
                        List<Object> list) {
    String json = byteBuf.toString(StandardCharsets.UTF_8).trim();
    list.add(JSON.parseObject(json, MessageTransportVo.class));
    byteBuf.skipBytes(byteBuf.readableBytes());
  }
}
