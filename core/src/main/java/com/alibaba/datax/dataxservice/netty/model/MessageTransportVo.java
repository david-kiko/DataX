package com.alibaba.datax.dataxservice.netty.model;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.fastjson.JSON;

/**
 * 消息封装类
 */
public class MessageTransportVo {
  private int id;
  private int taskGroupId;
  private int type;
  private String message;
  private Communication communication;
  private String configuration;

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public int getTaskGroupId() {
    return taskGroupId;
  }

  public void setTaskGroupId(int taskGroupId) {
    this.taskGroupId = taskGroupId;
  }

  public int getType() {
    return type;
  }

  public void setType(int type) {
    this.type = type;
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  public Communication getCommunication() {
    return communication;
  }

  public void setCommunication(Communication communication) {
    this.communication = communication;
  }

  public String getConfiguration() {
    return configuration;
  }

  public void setConfiguration(String configuration) {
    this.configuration = configuration;
  }

  @Override
  public String toString() {
    return JSON.toJSONString(this);
  }
}
