package com.alibaba.datax.dataxservice.netty.enumeration;

import com.alibaba.datax.common.spi.ErrorCode;

public enum NettyErrorCode implements ErrorCode {
  SERVER_START_ERROR("Netty-0", "Netty server start error"),
  RETRY_COUNT_ERROR("Netty-1", "Netty retry count has been used up"),
  MESSAGE_SEND_ERROR("Netty-2", "Netty message send failed"),
  SERVER_ADDRESS_ERROR("Netty-3", "Netty server ip not exists, check your environment"),
  CLIENT_CONNECT_ERROR("Netty-4", "Netty client connect failed, check server status"),
  CLIENT_CONNECT_BREAK("Netty-5", "Netty client connect broken, check client status"),
  CHANNEL_CLOSE_ERROR("Netty-6", "Netty channel close error")
  ;

  private final String code;

  private final String describe;

  NettyErrorCode(String code, String describe) {
    this.code = code;
    this.describe = describe;
  }

  @Override
  public String getCode() {
    return this.code;
  }

  @Override
  public String getDescription() {
    return this.describe;
  }
}
