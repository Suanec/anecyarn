package anec.ml.yarn.anecyarn.api;

import anec.ml.yarn.anecyarn.common.HeartbeatRequest;
import anec.ml.yarn.anecyarn.common.HeartbeatResponse;
import anec.ml.yarn.anecyarn.common.InputInfo;
import anec.ml.yarn.anecyarn.common.OutputInfo;
import anec.ml.yarn.anecyarn.container.AnecYarnContainerId;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.mapred.InputSplit;

public interface ApplicationContainerProtocol extends VersionedProtocol {

  public static final long versionID = 1L;

  void reportReservedPort(String host, int port, String role, int index);

  void reportLightGbmIpPort(AnecYarnContainerId containerId, String lightGbmIpPort);

  String getLightGbmIpPortStr();

  void reportLightLDAIpPort(AnecYarnContainerId containerId, String lightLDAIpPort);

  String getLightLDAIpPortStr();

  String getClusterDef();

  HeartbeatResponse heartbeat(AnecYarnContainerId containerId, HeartbeatRequest heartbeatRequest);

  InputInfo[] getInputSplit(AnecYarnContainerId containerId);

  InputSplit[] getStreamInputSplit(AnecYarnContainerId containerId);

  OutputInfo[] getOutputLocation();

  void reportTensorBoardURL(String url);

  void reportMapedTaskID(AnecYarnContainerId containerId, String taskId);

  void reportCpuMetrics(AnecYarnContainerId containerId, String cpuMetrics);

  Long interResultTimeStamp();

}
