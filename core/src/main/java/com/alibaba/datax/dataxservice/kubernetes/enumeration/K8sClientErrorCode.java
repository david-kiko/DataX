package com.alibaba.datax.dataxservice.kubernetes.enumeration;

import com.alibaba.datax.common.spi.ErrorCode;

public enum K8sClientErrorCode implements ErrorCode {
  K8S_CLIENT_INIT_ERROR_CODE("K8S-0", "K8S Client init error"),
  DOCKER_IMAGES_ENV_NOT_EXISTS("K8S-1", "docker registry(task groups images) not in environment"),
  NAMESPACE_ENV_NOT_EXISTS("K8S-2", "namespace not in environment"),
  DATAX_FILE_WRITE_ERROR("K8S-3", "datax json file write error"),
  DATAX_VOLUME_ENV_NOT_EXISTS("K8S-4", "nfs volume not in environment"),
  POD_GROUP_STATUS_TIME_OUT("K8S-5", "volcano podGroup takes too long to start"),
  POD_GROUP_NOT_FOUND("K8S-6", "volcano podGroup not found too long")
  ;

  private final String code;

  private final String describe;

  K8sClientErrorCode(String code, String describe) {
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
