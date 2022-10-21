package com.alibaba.datax.core.statistics.container.collector;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.LocalTGCommunicationManager;
import com.alibaba.datax.dataxservice.face.domain.enums.State;
import com.alibaba.datax.dataxservice.netty.enumeration.NettyErrorCode;
import com.alibaba.datax.dataxservice.netty.server.ServerChannelHandlerPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessOuterCollector extends AbstractCollector {
    private static final Logger LOG = LoggerFactory.getLogger(ProcessOuterCollector.class);

    public ProcessOuterCollector(Long jobId) {
        super.setJobId(jobId);
    }

    @Override
    public Communication collectFromTaskGroup() {
        Communication jobCommunication = LocalTGCommunicationManager.getJobCommunication();
        // driver将executor传递的communication数据进行汇总，这一步并不是直接netty调用，而是executor communication->driver 本地缓存-> driver 汇总communication
        if (ServerChannelHandlerPool.CHANNEL_MAP.isEmpty() && jobCommunication.getState() == State.RUNNING){
            // 汇总的时候做个判断，消息通道CHANNEL_MAP是否全部为空，如果为空但是Communication还是running状态，则说明有连接中途断掉了，修改状态，避免driver无限循环等待
            jobCommunication.setState(State.FAILED);
            jobCommunication.setThrowable(new DataXException(NettyErrorCode.CLIENT_CONNECT_BREAK, "检测到所有client连接已断开，请联系IT管理员查看client异常"));
        }
        return jobCommunication;
    }
}
