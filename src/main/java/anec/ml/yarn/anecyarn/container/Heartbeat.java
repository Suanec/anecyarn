package anec.ml.yarn.anecyarn.container;

import anec.ml.yarn.anecyarn.api.ApplicationContainerProtocol;
import anec.ml.yarn.anecyarn.api.AnecYarnConstants;
import anec.ml.yarn.anecyarn.common.HeartbeatRequest;
import anec.ml.yarn.anecyarn.common.HeartbeatResponse;
import anec.ml.yarn.anecyarn.common.OutputInfo;
import anec.ml.yarn.anecyarn.common.AnecYarnContainerStatus;
import anec.ml.yarn.anecyarn.conf.AnecYarnConfiguration;
import anec.ml.yarn.anecyarn.util.Utilities;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Heartbeat extends Thread {

  private static final Log LOG = LogFactory.getLog(Heartbeat.class);

  private ApplicationContainerProtocol protocol;

  private Configuration conf;

  private AnecYarnContainerId containerId;

  private HeartbeatRequest heartbeatRequest;

  private HeartbeatResponse heartbeatResponse;

  private int heartbeatInterval;

  private int heartbeatRetryMax;

  private Long lastInnerModelTimeStamp;

  private Boolean IsAnecYarnTrainCompleted;

  private int outputIndex;

  private int index;

  private String role;

  public Heartbeat(ApplicationContainerProtocol protocol, Configuration conf,
                   AnecYarnContainerId anecyarnContainerId, int outputIndex, int index, String role) {
    this.protocol = protocol;
    this.conf = conf;
    this.containerId = anecyarnContainerId;
    this.heartbeatRequest = new HeartbeatRequest();
    this.heartbeatResponse = new HeartbeatResponse();
    this.lastInnerModelTimeStamp = Long.MIN_VALUE;
    this.IsAnecYarnTrainCompleted = false;
    this.heartbeatInterval = this.conf.getInt(AnecYarnConfiguration.ANECYARN_CONTAINER_HEARTBEAT_INTERVAL, AnecYarnConfiguration.DEFAULT_ANECYARN_CONTAINER_HEARTBEAT_INTERVAL);
    this.heartbeatRetryMax = this.conf.getInt(AnecYarnConfiguration.ANECYARN_CONTAINER_HEARTBEAT_RETRY, AnecYarnConfiguration.DEFAULT_ANECYARN_CONTAINER_HEARTBEAT_RETRY);
    this.outputIndex = outputIndex;
    this.index = index;
    this.role = role;
  }

  @SuppressWarnings("static-access")
  public void run() {
    while (!Thread.currentThread().interrupted()) {
      heartbeatResponse = heartbeatWithRetry();
      heartbeatResponseHandle(heartbeatResponse);
      Utilities.sleep(heartbeatInterval);
    }
  }

  public void setContainerStatus(AnecYarnContainerStatus containerStatus) {
    this.heartbeatRequest.setAnecYarnContainerStatus(containerStatus);
  }

  public void setInnerModelSavedStatus(Boolean flag) {
    this.heartbeatRequest.setInnerModelSavedStatus(flag);
  }

  public void setProgressLog(String anecyarnProgressLog) {
    this.heartbeatRequest.setProgressLog(anecyarnProgressLog);
  }

  public void setContainersStartTime(String startTime) {
    this.heartbeatRequest.setContainersStartTime(startTime);
  }

  public void setContainersFinishTime(String finishTime) {
    this.heartbeatRequest.setContainersFinishTime(finishTime);
  }

  public Boolean isAnecYarnTrainCompleted() {
    return this.IsAnecYarnTrainCompleted;
  }

  public HeartbeatResponse heartbeatWithRetry() {
    int retry = 0;
    while (true) {
      try {
        heartbeatResponse = protocol.heartbeat(containerId, heartbeatRequest);
        LOG.debug("Send HeartBeat to ApplicationMaster");
        return heartbeatResponse;
      } catch (Exception e) {
        retry++;
        if (retry <= heartbeatRetryMax) {
          LOG.warn("Send heartbeat to ApplicationMaster failed in retry " + retry);
          Utilities.sleep(heartbeatInterval);
        } else {
          LOG.warn("Send heartbeat to ApplicationMaster failed in retry " + retry
              + ", container will suicide!", e);
          System.exit(1);
        }
      }
    }
  }

  public void heartbeatResponseHandle(HeartbeatResponse heartbeatResponse) {
    LOG.debug("Received the heartbeat response from the AM. CurrentJob finished " + heartbeatResponse.getIsAnecYarnTrainCompleted()
        + " , currentInnerModelSavedTimeStamp is " + heartbeatResponse.getInnerModelTimeStamp());
    if (!heartbeatResponse.getIsAnecYarnTrainCompleted()) {
      if (!heartbeatResponse.getInnerModelTimeStamp().equals(lastInnerModelTimeStamp)) {
        lastInnerModelTimeStamp = heartbeatResponse.getInnerModelTimeStamp();
        final Thread interResultSavedThread = new Thread(new Runnable() {
          @Override
          public void run() {
            try {
              for (OutputInfo outputs : protocol.getOutputLocation()) {
                LOG.info("Output path: " + outputs.getLocalLocation() + "#" + outputs.getDfsLocation());
                FileSystem localFs = FileSystem.getLocal(conf);
                Path localPath = new Path(outputs.getLocalLocation());
                Path remotePath = new Path(outputs.getDfsLocation()
                    + conf.get(AnecYarnConfiguration.ANECYARN_INTERREAULST_DIR, AnecYarnConfiguration.DEFAULT_ANECYARN_INTERRESULT_DIR)
                    + new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss").format(new Date(lastInnerModelTimeStamp))
                    + "/" + containerId.toString());
                if (outputIndex >= 0) {
                  if (role.equals(AnecYarnConstants.WORKER) && index == outputIndex)
                    remotePath = new Path(outputs.getDfsLocation()
                        + conf.get(AnecYarnConfiguration.ANECYARN_INTERREAULST_DIR, AnecYarnConfiguration.DEFAULT_ANECYARN_INTERRESULT_DIR)
                        + new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss").format(new Date(lastInnerModelTimeStamp))
                        + "/" + localPath);
                  else
                    break;
                }
                LOG.info("InnerModel path:" + remotePath);
                FileSystem dfs = remotePath.getFileSystem(conf);
                if (dfs.exists(remotePath)) {
                  LOG.info("Container remote output path " + remotePath + "exists, so we has to delete is first.");
                  dfs.delete(remotePath);
                }
                if (localFs.exists(localPath)) {
                  LOG.info("Start upload output " + localPath + " to remote path " + remotePath);
                  dfs.copyFromLocalFile(false, false, localPath, remotePath);
                  LOG.info("Upload output " + localPath + " to remote path " + remotePath + " finished.");
                }
                localFs.close();
              }
              LOG.info("container " + containerId + " currentStatus:" + heartbeatRequest.getAnecYarnContainerStatus() + " , savedModel completed");
            } catch (Exception e) {
              LOG.error("upload the interResult error:" + e);
            } finally {
              Long timeInterval = System.currentTimeMillis() - lastInnerModelTimeStamp;
              if (timeInterval <= conf.getInt(AnecYarnConfiguration.ANECYARN_INTERRESULT_UPLOAD_TIMEOUT, AnecYarnConfiguration.DEFAULT_ANECYARN_INTERRESULT_UPLOAD_TIMEOUT)) {
                setInnerModelSavedStatus(true);
              }
            }
          }
        });
        interResultSavedThread.start();
      } else if (!lastInnerModelTimeStamp.equals(Long.MIN_VALUE)) {
        Long timeInterval = System.currentTimeMillis() - lastInnerModelTimeStamp;
        if (timeInterval > conf.getInt(AnecYarnConfiguration.ANECYARN_INTERRESULT_UPLOAD_TIMEOUT, AnecYarnConfiguration.DEFAULT_ANECYARN_INTERRESULT_UPLOAD_TIMEOUT)) {
          setInnerModelSavedStatus(true);
        }
      }
    }
    this.IsAnecYarnTrainCompleted = heartbeatResponse.getIsAnecYarnTrainCompleted();
  }
}
