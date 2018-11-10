package anec.ml.yarn.anecyarn.api;

import anec.ml.yarn.anecyarn.common.InputInfo;
import anec.ml.yarn.anecyarn.common.Message;
import anec.ml.yarn.anecyarn.common.OutputInfo;
import anec.ml.yarn.anecyarn.common.AnecYarnContainerStatus;
import anec.ml.yarn.anecyarn.container.AnecYarnContainerId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.mapred.InputSplit;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public interface ApplicationContext {

  ApplicationId getApplicationID();

  int getWorkerNum();

  int getPsNum();

  int getWorkerMemory();

  int getPsMemory();

  int getWorkerVCores();

  int getPsVCores();

  List<Container> getWorkerContainers();

  List<Container> getPsContainers();

  AnecYarnContainerStatus getContainerStatus(AnecYarnContainerId containerId);

  List<InputInfo> getInputs(AnecYarnContainerId containerId);

  List<InputSplit> getStreamInputs(AnecYarnContainerId containerId);

  List<OutputInfo> getOutputs();

  LinkedBlockingQueue<Message> getMessageQueue();

  String getTensorBoardUrl();

  Map<AnecYarnContainerId, String> getReporterProgress();

  Map<AnecYarnContainerId, String> getContainersAppStartTime();

  Map<AnecYarnContainerId, String> getContainersAppFinishTime();

  Map<AnecYarnContainerId, String> getMapedTaskID();

  Map<AnecYarnContainerId, ConcurrentHashMap<String, LinkedBlockingDeque<Object>>> getContainersCpuMetrics();

  Map<AnecYarnContainerId, ConcurrentHashMap<String, List<Double>>> getContainersCpuStatistics();

  int getSavingModelStatus();

  int getSavingModelTotalNum();

  Boolean getStartSavingStatus();

  void startSavingModelStatus(Boolean flag);

  Boolean getLastSavingStatus();

  List<Long> getModelSavingList();

  String getTfEvaluatorId();

}
