package com.alibaba.datax.dataxservice.netty.enumeration;

public enum MessageTypeEnum {
  CONNECT_INIT(1, "connect init bind taskGroupId", 1),
  UPDATE_COMMUNICATION(2, "update job communication", 1),
  MESSAGE_RESPONSE(3, "Message accepted successfully", 0),
  JOB_KILL(4, "kill task group job", 2),
  CONNECT_HEARTBEAT(5, "connect heart beat message", 1),
  ;

  MessageTypeEnum(int type, String desc, int use) {
    this.type = type;
    this.desc = desc;
    this.use = use;
  }
  // 消息类型
  private final int type;
  // 消息类型作用描述
  private final String desc;
  // 该消息类型是给谁用的 0=common,1=Server,2=Client
  private final int use;

  public int getType() {
    return type;
  }

  public String getDesc() {
    return desc;
  }

  public int getUse() {
    return use;
  }

  /**
   * 判断该类型是否在Server接收消息的类型中
   *
   * @param type 类型
   * @return 判断
   */
  public static boolean typeExistsInServer(int type) {
    boolean isExists = false;
    for (MessageTypeEnum typeEnum : MessageTypeEnum.values()) {
      if (typeEnum.getUse() == 0 || typeEnum.getUse() == 1 && typeEnum.getType() == type) {
        isExists = true;
        break;
      }
    }
    return isExists;
  }

  /**
   * 判断该类型是否在Client接收消息的类型中
   *
   * @param type 类型
   * @return 判断
   */
  public static boolean typeExistsInClient(int type) {
    boolean isExists = false;
    for (MessageTypeEnum typeEnum : MessageTypeEnum.values()) {
      if (typeEnum.getUse() == 0 || typeEnum.getUse() == 2 && typeEnum.getType() == type) {
        isExists = true;
        break;
      }
    }
    return isExists;
  }
}
