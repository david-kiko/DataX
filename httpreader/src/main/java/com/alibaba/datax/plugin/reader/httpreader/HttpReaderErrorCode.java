package com.alibaba.datax.plugin.reader.httpreader;

import com.alibaba.datax.common.spi.ErrorCode;

public enum HttpReaderErrorCode implements ErrorCode {
    BAD_CONFIG_VALUE("HttpReader-00", "您配置的值不合法."),
    PATH_NOT_FIND_ERROR("HttpReader-01", "您未配置path值"),
    DEFAULT_FS_NOT_FIND_ERROR("HttpReader-02", "您未配置defaultFS值"),
    ILLEGAL_VALUE("HttpReader-03", "值错误"),
    CONFIG_INVALID_EXCEPTION("HttpReader-04", "参数配置错误"),
    REQUIRED_VALUE("HttpReader-05", "您缺失了必须填写的参数值."),
    NO_INDEX_VALUE("HttpReader-06","没有 Index" ),
    MIXED_INDEX_VALUE("HttpReader-07","index 和 value 混合" ),
    EMPTY_DIR_EXCEPTION("HttpReader-08", "您尝试读取的文件目录为空."),
    PATH_CONFIG_ERROR("HttpReader-09", "您配置的path格式有误"),
    READ_FILE_ERROR("HttpReader-10", "读取文件出错"),
    MALFORMED_ORC_ERROR("HttpReader-10", "ORCFILE格式异常"),
    FILE_TYPE_ERROR("HttpReader-11", "文件类型配置错误"),
    FILE_TYPE_UNSUPPORT("HttpReader-12", "文件类型目前不支持"),
    KERBEROS_LOGIN_ERROR("HttpReader-13", "KERBEROS认证失败"),
    READ_SEQUENCEFILE_ERROR("HttpReader-14", "读取SequenceFile文件出错"),
    READ_RCFILE_ERROR("HttpReader-15", "读取RCFile文件出错"),
    INIT_RCFILE_SERDE_ERROR("HttpReader-16", "Deserialize RCFile, initialization failed!"),
    PARSE_MESSAGE_TYPE_FROM_SCHEMA_ERROR("HttpReader-17", "Error parsing ParquetSchema"),
    INVALID_PARQUET_SCHEMA("HttpReader-18", "ParquetSchema is invalid"),
    READ_PARQUET_ERROR("HttpReader-19", "Error reading Parquet file"),
    CONNECT_HDFS_IO_ERROR("HttpReader-20", "I/O exception in establishing connection with HDFS");

    private final String code;
    private final String description;

    private HttpReaderErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public String toString() {
        return String.format("Code:[%s], Description:[%s]. ", this.code,
                this.description);
    }
}