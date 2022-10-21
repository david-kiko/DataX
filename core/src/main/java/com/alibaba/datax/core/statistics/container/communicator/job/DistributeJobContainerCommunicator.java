package com.alibaba.datax.core.statistics.container.communicator.job;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.CommunicationTool;
import com.alibaba.datax.core.statistics.container.collector.ProcessOuterCollector;
import com.alibaba.datax.core.statistics.container.communicator.AbstractContainerCommunicator;
import com.alibaba.datax.core.statistics.container.report.ProcessOuterReporter;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.dataxservice.face.domain.enums.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class DistributeJobContainerCommunicator extends AbstractContainerCommunicator {
    private static final Logger LOG = LoggerFactory
            .getLogger(DistributeJobContainerCommunicator.class);

    /**
     * 初始化配置参数，communication收集类，communication报告类
     * 这里和standAlone模式基本一样，只有实现的的communication收集类，communication报告类不一样
     *
     * @param configuration 配置参数
     */
    public DistributeJobContainerCommunicator(Configuration configuration) {
        super(configuration);
        super.setCollector(new ProcessOuterCollector(configuration.getLong(
                CoreConstant.DATAX_CORE_CONTAINER_JOB_ID)));
        super.setReporter(new ProcessOuterReporter());
    }

    @Override
    public void registerCommunication(List<Configuration> configurationList) {
        super.getCollector().registerTGCommunication(configurationList);
    }

    @Override
    public Communication collect() {
        return super.getCollector().collectFromTaskGroup();
    }

    @Override
    public State collectState() {
        return this.collect().getState();
    }

    /**
     * 和 DistributeJobContainerCollector 的 report 实现一样
     */
    @Override
    public void report(Communication communication) {
        super.getReporter().reportJobCommunication(super.getJobId(), communication);

        LOG.info(CommunicationTool.Stringify.getSnapshot(communication));
        reportVmInfo();
    }

    @Override
    public Communication getCommunication(Integer taskGroupId) {
        return super.getCollector().getTGCommunication(taskGroupId);
    }

    @Override
    public Map<Integer, Communication> getCommunicationMap() {
        return super.getCollector().getTGCommunicationMap();
    }
}
