package com.alibaba.datax.core.job.scheduler.processouter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.job.scheduler.AbstractScheduler;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.LocalTGCommunicationManager;
import com.alibaba.datax.core.statistics.container.communicator.AbstractContainerCommunicator;
import com.alibaba.datax.core.taskgroup.TaskGroupContainer;
import com.alibaba.datax.core.taskgroup.runner.TaskGroupContainerRunner;
import com.alibaba.datax.core.util.FileUtil;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.dataxservice.face.domain.enums.State;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.File;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public abstract class ProcessOuterScheduler extends AbstractScheduler {

    private static final Logger log = LoggerFactory.getLogger(ProcessOuterScheduler.class);

    private ExecutorService taskGroupContainerExecutorService;

    public ProcessOuterScheduler(AbstractContainerCommunicator containerCommunicator) {
        super(containerCommunicator);
    }

    @Override
    public void startAllTaskGroup(List<Configuration> configurations) {
//        this.taskGroupContainerExecutorService = Executors
//                .newFixedThreadPool(configurations.size());
//
        Integer cnt = 0;
        for (Configuration taskGroupConfiguration : configurations) {
//            TaskGroupContainerRunner taskGroupContainerRunner = newTaskGroupContainerRunner(taskGroupConfiguration);
//            this.taskGroupContainerExecutorService.execute(taskGroupContainerRunner);

            taskGroupConfiguration.set(CoreConstant.DATAX_CORE_CONTAINER_MODEL, "taskGroup");
            String taskGroupFileName = "taskGroup-" + cnt;
            String absTaskFileName = StringUtils.join(new String[] {
                    CoreConstant.DATAX_HOME, "result", taskGroupFileName}, File.separator);
            try {
                FileUtil.writeFile(absTaskFileName, taskGroupConfiguration.toString(), false);
            } catch (Exception e) {
                e.printStackTrace();
            }
            cnt ++;
        }

        Boolean completeFlag = false;
        while(!completeFlag) {
            log.info("wait taskGroup completed...");
            for (int i = 0; i < cnt; i++) {
                String finishFileName = "finish-" + i;
                String absFinishFileName = StringUtils.join(new String[]{
                        CoreConstant.DATAX_HOME, "result", finishFileName}, File.separator);
                if (FileUtil.fileExists(absFinishFileName)) {
                    try {
                        String finishState = FileUtil.readToString(absFinishFileName);
                        Communication communication = LocalTGCommunicationManager.getTaskGroupCommunication(i);
                        State state = State.FAILED;
                        switch (finishState) {
                            case "40":
                                state = State.KILLING; break;
                            case "50":
                                state = State.KILLED; break;
                            case "60":
                                state = State.FAILED; break;
                            case "70":
                                state = State.SUCCEEDED; break;
                            default:
                                break;
                        }
                        communication.setState(state);
                        LocalTGCommunicationManager.updateTaskGroupCommunication(i, communication);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    completeFlag = true;
                } else {
                    completeFlag = false;
                    break;
                }
            }

            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
//
//        this.taskGroupContainerExecutorService.shutdown();
    }

    @Override
    public void dealFailedStat(AbstractContainerCommunicator frameworkCollector, Throwable throwable) {
        this.taskGroupContainerExecutorService.shutdownNow();
        throw DataXException.asDataXException(
                FrameworkErrorCode.PLUGIN_RUNTIME_ERROR, throwable);
    }


    @Override
    public void dealKillingStat(AbstractContainerCommunicator frameworkCollector, int totalTasks) {
        //通过进程退出返回码标示状态
        this.taskGroupContainerExecutorService.shutdownNow();
        throw DataXException.asDataXException(FrameworkErrorCode.KILLED_EXIT_VALUE,
                "job killed status");
    }


    private TaskGroupContainerRunner newTaskGroupContainerRunner(
            Configuration configuration) {
        TaskGroupContainer taskGroupContainer = new TaskGroupContainer(configuration);

        return new TaskGroupContainerRunner(taskGroupContainer);
    }

}
