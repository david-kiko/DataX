package com.alibaba.datax.dataxservice.kubernetes.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.dataxservice.kubernetes.enumeration.K8sClientErrorCode;
import com.alibaba.datax.dataxservice.netty.enumeration.NettyConstant;
import io.fabric8.kubernetes.client.*;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.volcano.client.DefaultVolcanoClient;
import io.fabric8.volcano.client.NamespacedVolcanoClient;
import io.fabric8.volcano.scheduling.v1beta1.PodGroup;
import io.fabric8.volcano.scheduling.v1beta1.PodGroupCondition;
import io.fabric8.volcano.scheduling.v1beta1.PodGroupList;
import io.fabric8.volcano.scheduling.v1beta1.PodGroupStatus;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class K8sClientUtil {
  private K8sClientUtil() {
  }

  private static final Logger LOG = LoggerFactory.getLogger(K8sClientUtil.class);

  /**
   * 客户端
   */
  private static KubernetesClient client;

  /**
   * 获取k8s客户端，创建时直接指定namespace
   *
   * @return k8s客户端
   */
  public static KubernetesClient getClient() {
    if (client == null){
      String namespace = System.getenv(NettyConstant.NAMESPACE);
      if (StringUtils.isBlank(namespace)){
        throw new DataXException(K8sClientErrorCode.NAMESPACE_ENV_NOT_EXISTS, "环境变量缺失namespace参数，请检查环境变量设置");
      }
      Config config = new ConfigBuilder().withNamespace(namespace).build();
      client = new KubernetesClientBuilder().withConfig(config).build();
    }
    return client;
  }

  /**
   * 获取volcano client,主要用来操作podGroup和queue
   *
   * @return volcano client
   */
  public static NamespacedVolcanoClient getVolcanoClient(){
    return new DefaultVolcanoClient(getClient());
  }

  /**
   * 监控podGroup创建状态
   *
   * @param executorMap vcJob参数
   * @param timeOutSeconds 超时报错时间，对象为秒
   * @param targetStatus "Running"
   */
  public static void waitUntilCondition(Map<String, Object> executorMap, Long timeOutSeconds,String targetStatus){
    String groupName = ((Map<String, String>) executorMap.get("metadata")).get("name");
    MixedOperation<PodGroup, PodGroupList, Resource<PodGroup>>
        podGroups = getVolcanoClient().podGroups();
    long start = System.currentTimeMillis();
    try {
      // 等待60秒,避免podGroup还未创建，volcano创建vcJob后会创建同名podGroup，中间可能会有少许延迟
      podGroups.waitUntilCondition(podGroup -> podGroup!= null && groupName.equals(podGroup.getMetadata().getName()), timeOutSeconds,TimeUnit.SECONDS);
    } catch (KubernetesClientTimeoutException e) {
      // driver报错的话，executor的vcJob需要删除一下，避免driver已经报错结束了，结果executor又有资源起来了
      deleteVcJob(executorMap);
      throw DataXException.asDataXException(K8sClientErrorCode.POD_GROUP_NOT_FOUND, String.format("podGroup未找到，请确认是否正确创建,查找时间：%d秒，groupName：%s", timeOutSeconds, groupName), e);
    }
    Resource<PodGroup> podGroup = podGroups.withName(groupName);
    timeOutSeconds = timeOutSeconds - ((System.currentTimeMillis() - start) / 1000);
    try {
      // 等待【timeOutSeconds】秒 或者 podGroup的状态变成【targetStatus】
      podGroup.waitUntilCondition(
          group -> Objects.nonNull(group.getStatus()) && targetStatus.equals(group.getStatus().getPhase()),
          timeOutSeconds,
          TimeUnit.SECONDS);
    } catch (KubernetesClientTimeoutException e) {
      PodGroupStatus podGroupStatus = podGroup.get().getStatus();
      String status = podGroupStatus.getPhase();
      List<PodGroupCondition> conditions = podGroupStatus.getConditions();
      // driver报错的话，executor的vcJob需要删除一下，避免driver已经报错结束了，结果executor又有资源起来了
      deleteVcJob(executorMap);
      throw new DataXException(K8sClientErrorCode.POD_GROUP_STATUS_TIME_OUT, String.format("executor启动超时，超时时间%d秒，当前podGroup状态：%s，超时原因：%s", timeOutSeconds, status, conditions));
    }
  }

  public static PodGroupStatus getPodGroupStatus(Map<String, Object> executorMap){
    String groupName = ((Map<String, String>) executorMap.get("metadata")).get("name");
    Resource<PodGroup> podGroupResource = getVolcanoClient().podGroups().withName(groupName);
    if (podGroupResource != null && podGroupResource.get()!= null){
      return podGroupResource.get().getStatus();
    }
    return null;
  }


  /**
   * 初始化executor启动参数，实际上就是创建volcano job使用的yaml文件
   *
   * @param configurations executor配置参数
   * @return map集合，用来解析成yaml
   */
  public static Map<String,Object> initDataXJobMap(List<Configuration> configurations){
    Integer jobId = configurations.get(0).getInt(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID);
    String driverAddress = configurations.get(0).getString(CoreConstant.DATAX_CORE_DATAXSERVER_ADDRESS);
    Map<String, Object> map = newInstanceLinkedHashMap("apiVersion","batch.volcano.sh/v1alpha1");
    map.put("kind","Job");
    // jobId是唯一的，来自于etl-master
    Map<String, Object> metadata = newInstanceLinkedHashMap("name", "datax-distribute-" + jobId);
    map.put("metadata", metadata);
    // executor最少可用数量，这里要求全部可用
    Map<String, Object> spec =
        newInstanceLinkedHashMap("minAvailable", configurations.size());
    spec.put("schedulerName", "volcano");
    // 所有机构全部使用默认queue，不限制大小
    spec.put("queue", "default");

    List<Map<String, Object>> policies = new ArrayList<>();
    // 当pod被驱逐时，重启该job
    Map<String, Object> policy =
        newInstanceLinkedHashMap("event", "PodEvicted");
    policy.put("action","RestartJob");
    policies.add(policy);
    spec.put("policies",policies);

    // host挂载路径，主要是需要使用nfs
//    List<Map<String, Object>> volumes = new ArrayList<>();
//    String volumeName = "datax";
//    Map<String, Object> volume = newInstanceLinkedHashMap("name", volumeName);

//    String mountPath = System.getenv(NettyConstant.DATAX_MOUNT_PATH);
//    String hostPath = System.getenv((NettyConstant.DATAX_HOST_PATH));
//
//    if (StringUtils.isBlank(mountPath) || StringUtils.isBlank(hostPath)){
//      throw new DataXException(K8sClientErrorCode.DATAX_VOLUME_ENV_NOT_EXISTS, "容器环境变量未设置dataX文件挂载路径");
//    }
//    volume.put("hostPath",newInstanceLinkedHashMap("path",hostPath));
//    volumes.add(volume);
//
//    // executor mount挂载路径，为了获取driver写入的配置文件
//    List<Map<String, Object>> volumeMounts = new ArrayList<>();
//    Map<String, Object> volumeMount = newInstanceLinkedHashMap("name", volumeName);
//    volumeMount.put("mountPath", mountPath);
//    volumeMounts.add(volumeMount);
//
//    // 挂载krb5文件
//    Map<String, Object> krbVolume = newInstanceLinkedHashMap("name", "krb5-conf");
//    krbVolume.put("configMap", newInstanceLinkedHashMap("name", "krb5-config"));
//    volumes.add(krbVolume);
//
//    Map<String, Object> krbVolumeMount = newInstanceLinkedHashMap("name", "krb5-conf");
//    krbVolumeMount.put("readOnly", Boolean.TRUE);
//    krbVolumeMount.put("mountPath", "/etc/krb5.conf");
//    krbVolumeMount.put("subPath", "krb5.conf");
//    volumeMounts.add(krbVolumeMount);
//
//    Map<String, Object> krbFileVolume = newInstanceLinkedHashMap("name", "keytab");
//    krbFileVolume.put("hostPath",newInstanceLinkedHashMap("path","/home"));
//    volumes.add(krbFileVolume);
//
//    Map<String, Object> krbFileVolumeMount = newInstanceLinkedHashMap("name", "keytab");
//    krbFileVolumeMount.put("readOnly", Boolean.TRUE);
//    krbFileVolumeMount.put("mountPath", "/keytab");
//    volumeMounts.add(krbFileVolumeMount);

    List<Map<String,Object>> tasks = new ArrayList<>(configurations.size());
    for (Configuration configuration : configurations) {
      // 设置executor副本数量为1
      Map<String, Object> taskMap = newInstanceLinkedHashMap("replicas", 1);

      // 获取对应taskGroupId，拼接taskName，确保taskName唯一
      Integer taskGroupId = configuration.getInt(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID);
      String taskName = String.format("datax-job%d-taskgroup%d", jobId, taskGroupId);
      taskMap.put("name", taskName);

      // executor 的执行命令
      List<Object> commandList = new ArrayList<>();
      commandList.add("sh");
      commandList.add("-c");

      commandList.add(String.format("python /datax/bin/datax.py --jobid=%d --taskgroupid=%d --mode=distribute --driver=%s " +
                "--jvm=\"-XX:InitialRAMPercentage=80.0 -XX:MaxRAMPercentage=80.0\" %s;", jobId, taskGroupId, driverAddress, "/dev/null"));
      Map<String, Object> container = newInstanceLinkedHashMap("command", commandList);

      // 指定executor需要使用的镜像
      String images = System.getenv(NettyConstant.TASK_GROUP_DOCKER_IMAGE_ADDRESS);
      if (StringUtils.isBlank(images)){
        throw new DataXException(K8sClientErrorCode.DOCKER_IMAGES_ENV_NOT_EXISTS, "环境变量未设置taskGroup子任务images地址");
      }
      container.put("image",images);

      // 设置executor 名称
      container.put("name",taskName);

      // 设置executor资源大小
      Map<String, Object> limit = newInstanceLinkedHashMap("cpu", configuration.getString(CoreConstant.DATAX_JOB_SETTING_DISTRIBUTE_CPU, "0.5"));
      limit.put("memory", configuration.getString(CoreConstant.DATAX_JOB_SETTING_DISTRIBUTE_MEMORY, "1024Mi"));
      container.put("resources",newInstanceLinkedHashMap("limits",limit));
//      container.put("volumeMounts", volumeMounts);

      List<Map<String,Object>> containers = new ArrayList<>();
      containers.add(container);
      Map<String, Object> taskSpec =
          newInstanceLinkedHashMap("containers", containers);

//      taskSpec.put("volumes",volumes);

      // 设置重启策略，重不，失败直接failed
      taskSpec.put("restartPolicy","Never");

//      // 设置私有镜像仓库认证
//      List<Map<String, Object>> imagePullSecretsList = new ArrayList<>();
//      String imagesPullNames =
//          System.getenv().getOrDefault(NettyConstant.IMAGE_PULL_SECRETS, "devregistry");
//      for (String imagesPullName : imagesPullNames.split(",")) {
//        imagePullSecretsList.add(newInstanceLinkedHashMap("name", imagesPullName));
//      }
//      taskSpec.put("imagePullSecrets", imagePullSecretsList);

      Map<String, Object> temSpec = newInstanceLinkedHashMap("spec", taskSpec);
      // 打标签，晚上定时清理
      Map<String, Object> labels =
          newInstanceLinkedHashMap("podLifecycleType", "short-lived");
      labels.put("volcano.webbhook.enable", "true");
      temSpec.put("metadata", newInstanceLinkedHashMap("labels", labels));
      taskMap.put("template", temSpec);
      tasks.add(taskMap);
    }
    spec.put("tasks",tasks);
    map.put("spec",spec);

    return map;
  }

  /**
   * 创建一个LinkedHashMap集合，并初始化往里面放一个key-map
   *
   * @param key key
   * @param value value
   * @return LinkedHashMap集合
   */
  public static Map<String,Object> newInstanceLinkedHashMap(String key,Object value){
    LinkedHashMap<String, Object> map = new LinkedHashMap<>();
    map.put(key,value);
    return map;
  }

  /**
   * 写入文件到nfs供executor获取
   *
   * @param configuration 文件content
   * @param filePre 写入文件目录
   * @param taskName 文件名称
   * @return 文件绝对路径
   * @throws IOException 写入异常
   */
  public static String writeFile(Configuration configuration, String filePre,
                                 String taskName) throws IOException {
    File file = new File(filePre, taskName);
    FileUtils.writeStringToFile(file,configuration.toString(),false);
    return file.getPath();
  }

  /**
   * 创建volcano job
   *
   * @param vcJob 参数，用来解析成vcjob的yaml
   */
  public static void execVcJob(Map<String,Object> vcJob){
    Yaml yaml = new Yaml();
    String jobYaml = yaml.dump(vcJob);
    LOG.info("volcano yaml map:{}",vcJob);
    try {
      KubernetesClient client = getClient();
      client.load(new ByteArrayInputStream(jobYaml.getBytes(StandardCharsets.UTF_8))).createOrReplace();
    } catch (KubernetesClientException e) {
      if (e.getMessage().contains("Could not find a registered handler")) {
        LOG.error("Volcano service maybe not available，please check volcano!!!");
      }
      throw e;
    }
  }

  /**
   * 删除vcJob
   *
   * @param vcJob 参数
   */
  public static void  deleteVcJob(Map<String,Object> vcJob){
    Yaml yaml = new Yaml();
    String jobYaml = yaml.dump(vcJob);
    LOG.info("volcano yaml deleting");
    KubernetesClient client = getClient();
    client.load(new ByteArrayInputStream(jobYaml.getBytes(StandardCharsets.UTF_8))).delete();
  }
}
