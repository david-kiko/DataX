package com.alibaba.datax.core.job.scheduler.processouter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.job.scheduler.AbstractScheduler;
import com.alibaba.datax.core.statistics.container.communicator.AbstractContainerCommunicator;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.dataxservice.kubernetes.enumeration.K8sClientErrorCode;
import com.alibaba.datax.dataxservice.kubernetes.util.K8sClientUtil;
import com.alibaba.datax.dataxservice.netty.enumeration.MessageTypeEnum;
import com.alibaba.datax.dataxservice.netty.enumeration.NettyConstant;
import com.alibaba.datax.dataxservice.netty.model.MessageTransportVo;
import com.alibaba.datax.dataxservice.netty.server.ServerChannelHandlerPool;
import com.alibaba.datax.dataxservice.netty.util.SendMessageUtil;
import io.fabric8.volcano.scheduling.v1beta1.PodGroupStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public abstract class ProcessOuterScheduler extends AbstractScheduler {
    private static final Logger LOG = LoggerFactory.getLogger(ProcessOuterScheduler.class);

    public ProcessOuterScheduler(AbstractContainerCommunicator containerCommunicator) {
        super(containerCommunicator);
    }

    @Override
    public void startAllTaskGroup(List<Configuration> configurations) {
        // 使用k8s client将配置解析成volcano所需要的yaml文件，然后创建executor pod作业，一个configuration就是一个executor
        Map<String, Object> executorMap = K8sClientUtil.initDataXJobMap(configurations);
        K8sClientUtil.execVcJob(executorMap);

        // 等个10min，如果executor还没有连接上driver就直接报错
        long sleepTime = 1000L;
        int waitTime = Integer.parseInt(System.getenv().getOrDefault(NettyConstant.SERVER_WAIT_TIME,"600"));
        // 通过消息通道CHANNEL_MAP的数量来判断executor是否全部连接上了
        while (configurations.size() != ServerChannelHandlerPool.TASK_GROUP_ID_SET.size()){
            LOG.info("taskGroups:{} is ready，totalSize：{}, Wait for countdown:{}s,waiting...",
                ServerChannelHandlerPool.TASK_GROUP_ID_SET,configurations.size(), waitTime);
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.error("thread sleep error", e);
            }
            waitTime --;
            if (waitTime < 0){
                PodGroupStatus podGroupStatus = K8sClientUtil.getPodGroupStatus(executorMap);
                String message = String.format("超过倒计时，dataX client没有正常链接，请检查client状态，podGroupStatus：%s",
                        podGroupStatus);
                K8sClientUtil.deleteVcJob(executorMap);
                throw new DataXException(K8sClientErrorCode.K8S_CLIENT_INIT_ERROR_CODE, message);
            }
        }
        LOG.info("all taskGroup is ready, starting...");
    }

    /**
     * 有一个executor任务失败时，调用netty告诉所有的executor需要强行退出
     *
     * @param frameworkCollector ？
     * @param throwable 异常
     */
    @Override
    public void dealFailedStat(AbstractContainerCommunicator frameworkCollector, Throwable throwable) {
        sendKillCommand();
        throw DataXException.asDataXException(
                FrameworkErrorCode.PLUGIN_RUNTIME_ERROR, throwable);
    }

    private void sendKillCommand() {
        //通过进程退出返回码标示状态
        MessageTransportVo vo = new MessageTransportVo();
        vo.setType(MessageTypeEnum.JOB_KILL.getType());
        ServerChannelHandlerPool.CHANNEL_MAP.values().forEach(channel -> {
            try {
                SendMessageUtil.sendMessageByHandshakes(vo, channel);
            } catch (Exception e) {
                LOG.error("关闭Client消息发送失败", e);
            }
        });
    }


    /**
     * driver收到kill命令，调用netty告诉executor需要强行退出
     *
     * @param frameworkCollector ？
     * @param totalTasks task总量
     */
    @Override
    public void dealKillingStat(AbstractContainerCommunicator frameworkCollector, int totalTasks) {
        sendKillCommand();
        throw DataXException.asDataXException(FrameworkErrorCode.KILLED_EXIT_VALUE,
                "job killed status");
    }
}
