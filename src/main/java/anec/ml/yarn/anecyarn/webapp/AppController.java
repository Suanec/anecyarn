package anec.ml.yarn.anecyarn.webapp;

import anec.ml.yarn.anecyarn.api.AnecYarnConstants;
import anec.ml.yarn.anecyarn.common.OutputInfo;
import anec.ml.yarn.anecyarn.conf.AnecYarnConfiguration;
import anec.ml.yarn.anecyarn.container.AnecYarnContainerId;
import com.google.gson.*;
import com.google.inject.Inject;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.webapp.Controller;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebApps;

import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

import static org.apache.hadoop.yarn.util.StringHelper.join;

public class AppController extends Controller implements AMParams {

  private final Configuration conf;
  private final App app;

  @Inject
  public AppController(App app, Configuration conf, RequestContext ctx) {
    super(ctx);
    this.conf = conf;
    this.app = app;
    set(APP_ID, app.context.getApplicationID().toString());
    if (System.getenv().containsKey(AnecYarnConstants.Environment.ANECYARN_APP_TYPE.toString())) {
      if ("anecyarn".equals(System.getenv(AnecYarnConstants.Environment.ANECYARN_APP_TYPE.toString()).toLowerCase())) {
        set(APP_TYPE, "AnecYarn");
      } else {
        char[] appType = System.getenv(AnecYarnConstants.Environment.ANECYARN_APP_TYPE.toString()).toLowerCase().toCharArray();
        appType[0] -= 32;
        set(APP_TYPE, String.valueOf(appType));
      }
    } else {
      set(APP_TYPE, "AnecYarn");
    }

    String boardUrl = app.context.getTensorBoardUrl();
    if (this.conf.getBoolean(AnecYarnConfiguration.ANECYARN_TF_BOARD_ENABLE, AnecYarnConfiguration.DEFAULT_ANECYARN_TF_BOARD_ENABLE)) {
      if (boardUrl != null) {
        set(BOARD_INFO, boardUrl);
      } else {
        set(BOARD_INFO, "Waiting for board process start...");
      }
    } else {
      String boardInfo = "Board server don't start, You can set argument \"--board-enable true\" in your submit script to start.";
      set(BOARD_INFO, boardInfo);
    }

    List<Container> workerContainers = app.context.getWorkerContainers();
    List<Container> psContainers = app.context.getPsContainers();
    Map<AnecYarnContainerId, String> reporterProgress = app.context.getReporterProgress();
    Map<AnecYarnContainerId, String> containersAppStartTime = app.context.getContainersAppStartTime();
    Map<AnecYarnContainerId, String> containersAppFinishTime = app.context.getContainersAppFinishTime();
    set(CONTAINER_NUMBER, String.valueOf(workerContainers.size() + psContainers.size()));
    set(WORKER_NUMBER, String.valueOf(workerContainers.size()));
    set(PS_NUMBER, String.valueOf(psContainers.size()));
    set(WORKER_VCORES, String.valueOf(app.context.getWorkerVCores()));
    set(PS_VCORES, String.valueOf(app.context.getPsVCores()));
    set(WORKER_MEMORY, String.format("%.2f", app.context.getWorkerMemory() / 1024.0));
    set(PS_MEMORY, String.format("%.2f", app.context.getPsMemory() / 1024.0));
    set(USER_NAME, StringUtils.split(conf.get("hadoop.job.ugi"), ',')[0]);
    int i = 0;
    for (Container container : workerContainers) {
      set(CONTAINER_HTTP_ADDRESS + i, container.getNodeHttpAddress());
      set(CONTAINER_ID + i, container.getId().toString());
      if (app.context.getContainerStatus(new AnecYarnContainerId(container.getId())) != null) {
        set(CONTAINER_STATUS + i, app.context.getContainerStatus(new AnecYarnContainerId(container.getId())).toString());
      } else {
        set(CONTAINER_STATUS + i, "-");
      }
      if (conf.getBoolean(AnecYarnConfiguration.ANECYARN_TF_EVALUATOR, AnecYarnConfiguration.DEFAULT_ANECYARN_TF_EVALUATOR) && container.getId().toString().equals(app.context.getTfEvaluatorId())) {
        set(CONTAINER_ROLE + i, AnecYarnConstants.EVALUATOR);
      } else {
        set(CONTAINER_ROLE + i, AnecYarnConstants.WORKER);
      }

      if (app.context.getContainersCpuMetrics().get(new AnecYarnContainerId(container.getId())) != null) {
        ConcurrentHashMap<String, LinkedBlockingDeque<Object>> cpuMetrics = app.context.getContainersCpuMetrics().get(new AnecYarnContainerId(container.getId()));
        if (cpuMetrics.size() != 0) {
          set("cpuMemMetrics" + i, new Gson().toJson(cpuMetrics.get("CPUMEM")));
          if (cpuMetrics.containsKey("CPUUTIL")) {
            set("cpuUtilMetrics" + i, new Gson().toJson(cpuMetrics.get("CPUUTIL")));
          }
        }
        ConcurrentHashMap<String, List<Double>> cpuStatistics = app.context.getContainersCpuStatistics().get(new AnecYarnContainerId(container.getId()));
        if (cpuStatistics.size() != 0) {
          set(CONTAINER_CPU_STATISTICS_MEM + USAGE_AVG + i, String.format("%.2f", cpuStatistics.get("CPUMEM").get(0)));
          set(CONTAINER_CPU_STATISTICS_MEM + USAGE_MAX + i, String.format("%.2f", cpuStatistics.get("CPUMEM").get(1)));
          set(CONTAINER_CPU_STATISTICS_UTIL + USAGE_AVG + i, String.format("%.2f", cpuStatistics.get("CPUUTIL").get(0)));
          set(CONTAINER_CPU_STATISTICS_UTIL + USAGE_MAX + i, String.format("%.2f", cpuStatistics.get("CPUUTIL").get(1)));
        }
      }

      if (reporterProgress.get(new AnecYarnContainerId(container.getId())) != null && !reporterProgress.get(new AnecYarnContainerId(container.getId())).equals("")) {
        String progressLog = reporterProgress.get(new AnecYarnContainerId(container.getId()));
        String[] progress = progressLog.toString().split(":");
        if (progress.length != 2) {
          set(CONTAINER_REPORTER_PROGRESS + i, "progress log format error");
        } else {
          try {
            Float percentProgress = Float.parseFloat(progress[1]);
            if (percentProgress < 0.0 || percentProgress > 1.0) {
              set(CONTAINER_REPORTER_PROGRESS + i, "progress log format error");
            } else {
              DecimalFormat df = new DecimalFormat("0.00");
              df.setRoundingMode(RoundingMode.HALF_UP);
              set(CONTAINER_REPORTER_PROGRESS + i, df.format((Float.parseFloat(progress[1]) * 100)) + "%");
            }
          } catch (Exception e) {
            set(CONTAINER_REPORTER_PROGRESS + i, "progress log format error");
          }
        }
      } else {
        set(CONTAINER_REPORTER_PROGRESS + i, "0.00%");
      }
      if (containersAppStartTime.get(new AnecYarnContainerId(container.getId())) != null && !containersAppStartTime.get(new AnecYarnContainerId(container.getId())).equals("")) {
        String localStartTime = containersAppStartTime.get(new AnecYarnContainerId(container.getId()));
        set(CONTAINER_START_TIME + i, localStartTime);
      } else {
        set(CONTAINER_START_TIME + i, "N/A");
      }
      if (containersAppFinishTime.get(new AnecYarnContainerId(container.getId())) != null && !containersAppFinishTime.get(new AnecYarnContainerId(container.getId())).equals("")) {
        String localFinishTime = containersAppFinishTime.get(new AnecYarnContainerId(container.getId()));
        set(CONTAINER_FINISH_TIME + i, localFinishTime);
      } else {
        set(CONTAINER_FINISH_TIME + i, "N/A");
      }
      i++;
    }
    for (Container container : psContainers) {
      set(CONTAINER_HTTP_ADDRESS + i, container.getNodeHttpAddress());
      set(CONTAINER_ID + i, container.getId().toString());
      if (app.context.getContainerStatus(new AnecYarnContainerId(container.getId())) != null) {
        set(CONTAINER_STATUS + i, app.context.getContainerStatus(new AnecYarnContainerId(container.getId())).toString());
      } else {
        set(CONTAINER_STATUS + i, "-");
      }
      if ($(APP_TYPE).equals("Tensorflow")) {
        set(CONTAINER_ROLE + i, "ps");
      } else if ($(APP_TYPE).equals("Mxnet") || $(APP_TYPE).equals("Lightlda") || $(APP_TYPE).equals("Xflow")) {
        set(CONTAINER_ROLE + i, "server");
      }

      if (app.context.getContainersCpuMetrics().get(new AnecYarnContainerId(container.getId())) != null) {
        ConcurrentHashMap<String, LinkedBlockingDeque<Object>> cpuMetrics = app.context.getContainersCpuMetrics().get(new AnecYarnContainerId(container.getId()));
        if (cpuMetrics.size() != 0) {
          set("cpuMemMetrics" + i, new Gson().toJson(cpuMetrics.get("CPUMEM")));
          if (cpuMetrics.containsKey("CPUUTIL")) {
            set("cpuUtilMetrics" + i, new Gson().toJson(cpuMetrics.get("CPUUTIL")));
          }
        }
        ConcurrentHashMap<String, List<Double>> cpuStatistics = app.context.getContainersCpuStatistics().get(new AnecYarnContainerId(container.getId()));
        if (cpuStatistics.size() != 0) {
          set(CONTAINER_CPU_STATISTICS_MEM + USAGE_AVG + i, String.format("%.2f", cpuStatistics.get("CPUMEM").get(0)));
          set(CONTAINER_CPU_STATISTICS_MEM + USAGE_MAX + i, String.format("%.2f", cpuStatistics.get("CPUMEM").get(1)));
          set(CONTAINER_CPU_STATISTICS_UTIL + USAGE_AVG + i, String.format("%.2f", cpuStatistics.get("CPUUTIL").get(0)));
          set(CONTAINER_CPU_STATISTICS_UTIL + USAGE_MAX + i, String.format("%.2f", cpuStatistics.get("CPUUTIL").get(1)));
        }
      }

      set(CONTAINER_REPORTER_PROGRESS + i, "0.00%");
      if (containersAppStartTime.get(new AnecYarnContainerId(container.getId())) != null && !containersAppStartTime.get(new AnecYarnContainerId(container.getId())).equals("")) {
        String localStartTime = containersAppStartTime.get(new AnecYarnContainerId(container.getId()));
        set(CONTAINER_START_TIME + i, localStartTime);
      } else {
        set(CONTAINER_START_TIME + i, "N/A");
      }
      if (containersAppFinishTime.get(new AnecYarnContainerId(container.getId())) != null && !containersAppFinishTime.get(new AnecYarnContainerId(container.getId())).equals("")) {
        String localFinishTime = containersAppFinishTime.get(new AnecYarnContainerId(container.getId()));
        set(CONTAINER_FINISH_TIME + i, localFinishTime);
      } else {
        set(CONTAINER_FINISH_TIME + i, "N/A");
      }
      i++;
    }

    if (this.conf.get(AnecYarnConfiguration.ANECYARN_OUTPUT_STRATEGY, AnecYarnConfiguration.DEFAULT_ANECYARN_OUTPUT_STRATEGY).toUpperCase().equals("STREAM")) {
      set(OUTPUT_TOTAL, "0");
    } else {
      set(OUTPUT_TOTAL, String.valueOf(app.context.getOutputs().size()));
    }
    i = 0;
    for (OutputInfo output : app.context.getOutputs()) {
      Path interResult = new Path(output.getDfsLocation()
          + conf.get(AnecYarnConfiguration.ANECYARN_INTERREAULST_DIR, AnecYarnConfiguration.DEFAULT_ANECYARN_INTERRESULT_DIR));
      set(OUTPUT_PATH + i, interResult.toString());
      i++;
    }

    set(TIMESTAMP_TOTAL, String.valueOf(app.context.getModelSavingList().size()));
    int j = 0;
    for (i = app.context.getModelSavingList().size(); i > 0; i--) {
      set(TIMESTAMP_LIST + j, String.valueOf(app.context.getModelSavingList().get(i - 1)));
      j++;
    }

    set(CONTAINER_CPU_METRICS_ENABLE, String.valueOf(true));
    try {
      WebApps.Builder.class.getMethod("build", WebApp.class);
    } catch (NoSuchMethodException e) {
      if (Controller.class.getClassLoader().getResource("webapps/static/xlWebApp") == null) {
        LOG.debug("Don't have the xlWebApp Resource.");
        set(CONTAINER_CPU_METRICS_ENABLE, String.valueOf(false));
      }
    }

  }

  @Override
  public void index() {
    setTitle(join($(APP_TYPE) + " Application ", $(APP_ID)));
    if (app.context.getLastSavingStatus() && app.context.getStartSavingStatus() && app.context.getSavingModelStatus() == app.context.getSavingModelTotalNum()) {
      app.context.startSavingModelStatus(false);
    }
    set(SAVE_MODEL, String.valueOf(app.context.getStartSavingStatus()));
    set(SAVE_MODEL_STATUS, String.valueOf(app.context.getSavingModelStatus()));
    set(LAST_SAVE_STATUS, String.valueOf(app.context.getLastSavingStatus()));
    set(SAVE_MODEL_TOTAL, String.valueOf(app.context.getSavingModelTotalNum()));
    render(InfoPage.class);
  }

  public void savedmodel() {
    setTitle(join($(APP_TYPE) + " Application ", $(APP_ID)));
    set(SAVE_MODEL_STATUS, String.valueOf(app.context.getSavingModelStatus()));
    set(SAVE_MODEL_TOTAL, String.valueOf(app.context.getSavingModelTotalNum()));
    if (!app.context.getStartSavingStatus()) {
      app.context.startSavingModelStatus(true);
      set(SAVE_MODEL, String.valueOf(app.context.getStartSavingStatus()));
    } else {
      set(SAVE_MODEL, String.valueOf(app.context.getStartSavingStatus()));
    }
    render(InfoPage.class);
  }
}