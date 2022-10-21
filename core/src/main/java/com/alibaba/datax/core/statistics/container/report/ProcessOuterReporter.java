package com.alibaba.datax.core.statistics.container.report;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.dataxservice.face.domain.enums.State;
import com.alibaba.datax.dataxservice.netty.client.ClientChannelHandlerPool;
import com.alibaba.datax.dataxservice.netty.enumeration.MessageTypeEnum;
import com.alibaba.datax.dataxservice.netty.enumeration.NettyErrorCode;
import com.alibaba.datax.dataxservice.netty.model.MessageTransportVo;
import com.alibaba.datax.dataxservice.netty.util.SendMessageUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessOuterReporter extends AbstractReporter {
    private static final Logger LOG = LoggerFactory.getLogger(ProcessOuterReporter.class);
    private boolean isFinished = false;

    @Override
    public void reportJobCommunication(Long jobId, Communication communication) {
        // do nothing
    }

    /**
     * executor将communication记录发送给driver本地缓存
     *
     * @param taskGroupId executor id
     * @param communication 任务记录封装类
     */
    @Override
    public void reportTGCommunication(Integer taskGroupId, Communication communication) {
        MessageTransportVo vo = new MessageTransportVo();
        vo.setTaskGroupId(taskGroupId);
        vo.setType(MessageTypeEnum.UPDATE_COMMUNICATION.getType());
        vo.setCommunication(communication);
        LOG.info("MessageTransportVo:{}", vo);
        if (!isFinished && communication.getState() == State.SUCCEEDED){
            // 将第一次状态为success的communication修改为running，避免driver收到success信息提前就结束了，后面executor还会发送一次success信息，这次发送了driver才能结束
            communication.setState(State.RUNNING);
            this.isFinished = true;
        }
        boolean flag = SendMessageUtil.sendMessageByHandshakes(vo, ClientChannelHandlerPool.getChannel());
        if (flag){
            LOG.info("communication sent, state:{}, taskGroupId:{}",communication.getState() , taskGroupId);
        } else {
            throw new DataXException(NettyErrorCode.MESSAGE_SEND_ERROR,"Communication 信息发送失败");
        }
    }
}