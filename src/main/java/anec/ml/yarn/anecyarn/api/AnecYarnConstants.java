package anec.ml.yarn.anecyarn.api;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.ApplicationConstants;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface AnecYarnConstants {

  String ANECYARN_JOB_CONFIGURATION = "core-site.xml";

  String ANECYARN_APPLICATION_JAR = "AppMaster.jar";

  String WORKER = "worker";

  String PS = "ps";

  String EVALUATOR = "evaluator";

  String STREAM_INPUT_DIR = "mapreduce.input.fileinputformat.inputdir";

  String STREAM_OUTPUT_DIR = "mapreduce.output.fileoutputformat.outputdir";

  String AM_ENV_PREFIX = "anecyarn.am.env.";

  String CONTAINER_ENV_PREFIX = "anecyarn.container.env.";

  enum Environment {
    HADOOP_USER_NAME("HADOOP_USER_NAME"),

    ANECYARN_APP_TYPE("ANECYARN_APP_TYPE"),

    ANECYARN_APP_NAME("ANECYARN_APP_NAME"),

    ANECYARN_CONTAINER_MAX_MEMORY("ANECYARN_MAX_MEM"),

    ANECYARN_LIBJARS_LOCATION("ANECYARN_LIBJARS_LOCATION"),

    ANECYARN_TF_ROLE("TF_ROLE"),

    ANECYARN_TF_INDEX("TF_INDEX"),

    ANECYARN_TF_CLUSTER_DEF("TF_CLUSTER_DEF"),

    ANECYARN_DMLC_WORKER_NUM("DMLC_NUM_WORKER"),

    ANECYARN_DMLC_SERVER_NUM("DMLC_NUM_SERVER"),

    ANECYARN_LIGHTGBM_WORKER_NUM("LIGHTGBM_NUM_WORKER"),

    ANECYARN_LIGHTLDA_WORKER_NUM("LIGHTLDA_NUM_WORKER"),

    ANECYARN_LIGHTLDA_PS_NUM("LIGHTLDA_NUM_PS"),

    ANECYARN_INPUT_FILE_LIST("INPUT_FILE_LIST"),

    ANECYARN_STAGING_LOCATION("ANECYARN_STAGING_LOCATION"),

    ANECYARN_CACHE_FILE_LOCATION("ANECYARN_CACHE_FILE_LOCATION"),

    ANECYARN_CACHE_ARCHIVE_LOCATION("ANECYARN_CACHE_ARCHIVE_LOCATION"),

    ANECYARN_FILES_LOCATION("ANECYARN_FILES_LOCATION"),

    APP_JAR_LOCATION("APP_JAR_LOCATION"),

    ANECYARN_JOB_CONF_LOCATION("ANECYARN_JOB_CONF_LOCATION"),

    ANECYARN_EXEC_CMD("ANECYARN_EXEC_CMD"),

    USER_PATH("USER_PATH"),

    USER_LD_LIBRARY_PATH("USER_LD_LIBRARY_PATH"),

    ANECYARN_OUTPUTS("ANECYARN_OUTPUTS"),

    ANECYARN_OUTPUTS_WORKER_INDEX("ANECYARN_OUTPUT_WORKER_INDEX"),

    ANECYARN_INPUTS("ANECYARN_INPUTS"),

    ANECYARN_INPUT_PATH("ANECYARN_INPUT_PATH"),

    APPMASTER_HOST("APPMASTER_HOST"),

    APPMASTER_PORT("APPMASTER_PORT"),

    APP_ID("APP_ID"),

    APP_ATTEMPTID("APP_ATTEMPTID");

    private final String variable;

    Environment(String variable) {
      this.variable = variable;
    }

    public String key() {
      return variable;
    }

    public String toString() {
      return variable;
    }

    public String $() {
      if (Shell.WINDOWS) {
        return "%" + variable + "%";
      } else {
        return "$" + variable;
      }
    }

    @InterfaceAudience.Public
    @InterfaceStability.Unstable
    public String $$() {
      return ApplicationConstants.PARAMETER_EXPANSION_LEFT +
          variable +
          ApplicationConstants.PARAMETER_EXPANSION_RIGHT;
    }
  }
}
