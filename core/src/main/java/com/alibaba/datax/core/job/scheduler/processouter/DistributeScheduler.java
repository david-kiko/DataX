package com.alibaba.datax.core.job.scheduler.processouter;

import com.alibaba.datax.core.statistics.container.communicator.AbstractContainerCommunicator;

/**
 * Created by hankin.yang on 2022/07/26.
 */
public class DistributeScheduler extends ProcessOuterScheduler {

    public DistributeScheduler(AbstractContainerCommunicator containerCommunicator) {
        super(containerCommunicator);
    }

    @Override
    protected boolean isJobKilling(Long jobId) {
        return false;
    }

}
