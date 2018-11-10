package anec.ml.yarn.anecyarn.conf;

import anec.ml.yarn.anecyarn.common.TextMultiOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;

public class AnecYarnConfiguration extends YarnConfiguration {

  private static final String ANECYARN_DEFAULT_XML_FILE = "anecyarn-default.xml";

  private static final String ANECYARN_SITE_XML_FILE = "anecyarn-site.xml";

  static {
    YarnConfiguration.addDefaultResource(ANECYARN_DEFAULT_XML_FILE);
    YarnConfiguration.addDefaultResource(ANECYARN_SITE_XML_FILE);
  }

  public AnecYarnConfiguration() {
    super();
  }

  public AnecYarnConfiguration(Configuration conf) {
    super(conf);
  }

  /**
   * Configuration used in Client
   */
  public static final String DEFAULT_ANECYARN_APP_TYPE = "ANECYARN";

  public static final String ANECYARN_STAGING_DIR = "anecyarn.staging.dir";

  public static final String DEFAULT_ANECYARN_STAGING_DIR = "/tmp/AnecYarn/staging";

  public static final String ANECYARN_CONTAINER_EXTRAENV = "anecyarn.container.extraEnv";

  public static final String ANECYARN_LOG_PULL_INTERVAL = "anecyarn.log.pull.interval";

  public static final int DEFAULT_ANECYARN_LOG_PULL_INTERVAL = 10000;

  public static final String ANECYARN_USER_CLASSPATH_FIRST = "anecyarn.user.classpath.first";

  public static final boolean DEFAULT_ANECYARN_USER_CLASSPATH_FIRST = true;

  public static final String ANECYARN_HOST_LOCAL_ENABLE = "anecyarn.host.local.enable";

  public static final boolean DEFAULT_ANECYARN_HOST_LOCAL_ENABLE = false;

  public static final String ANECYARN_REPORT_CONTAINER_STATUS = "anecyarn.report.container.status";

  public static final boolean DEFAULT_ANECYARN_REPORT_CONTAINER_STATUS = true;

  public static final String ANECYARN_CONTAINER_MEM_USAGE_WARN_FRACTION = "anecyarn.container.mem.usage.warn.fraction";

  public static final Double DEFAULT_ANECYARN_CONTAINER_MEM_USAGE_WARN_FRACTION = 0.70;

  public static final String ANECYARN_AM_MEMORY = "anecyarn.am.memory";

  public static final int DEFAULT_ANECYARN_AM_MEMORY = 1024;

  public static final String ANECYARN_AM_CORES = "anecyarn.am.cores";

  public static final int DEFAULT_ANECYARN_AM_CORES = 1;

  public static final String ANECYARN_WORKER_MEMORY = "anecyarn.worker.memory";

  public static final int DEFAULT_ANECYARN_WORKER_MEMORY = 1024;

  public static final String ANECYARN_WORKER_VCORES = "anecyarn.worker.cores";

  public static final int DEFAULT_ANECYARN_WORKER_VCORES = 1;

  public static final String ANECYARN_WORKER_NUM = "anecyarn.worker.num";

  public static final int DEFAULT_ANECYARN_WORKER_NUM = 1;

  public static final String ANECYARN_PS_MEMORY = "anecyarn.ps.memory";

  public static final int DEFAULT_ANECYARN_PS_MEMORY = 1024;

  public static final String ANECYARN_PS_VCORES = "anecyarn.ps.cores";

  public static final int DEFAULT_ANECYARN_PS_VCORES = 1;

  public static final String ANECYARN_PS_NUM = "anecyarn.ps.num";

  public static final int DEFAULT_ANECYARN_PS_NUM = 0;

  public static final String ANECYARN_WORKER_MEM_AUTO_SCALE = "anecyarn.worker.mem.autoscale";

  public static final Double DEFAULT_ANECYARN_WORKER_MEM_AUTO_SCALE = 0.5;

  public static final String ANECYARN_PS_MEM_AUTO_SCALE = "anecyarn.ps.mem.autoscale";

  public static final Double DEFAULT_ANECYARN_PS_MEM_AUTO_SCALE = 0.2;

  public static final String ANECYARN_APP_MAX_ATTEMPTS = "anecyarn.app.max.attempts";

  public static final int DEFAULT_ANECYARN_APP_MAX_ATTEMPTS = 1;

  public static final String ANECYARN_MODE_SINGLE = "anecyarn.mode.single";

  public static Boolean DEFAULT_ANECYARN_MODE_SINGLE = false;

  public static final String ANECYARN_TF_EVALUATOR = "anecyarn.tf.evaluator";

  public static Boolean DEFAULT_ANECYARN_TF_EVALUATOR = false;

  public static final String ANECYARN_APP_QUEUE = "anecyarn.app.queue";

  public static final String DEFAULT_ANECYARN_APP_QUEUE = "DEFAULT";

  public static final String ANECYARN_APP_PRIORITY = "anecyarn.app.priority";

  public static final int DEFAULT_ANECYARN_APP_PRIORITY = 3;

  public static final String ANECYARN_OUTPUT_LOCAL_DIR = "anecyarn.output.local.dir";

  public static final String DEFAULT_ANECYARN_OUTPUT_LOCAL_DIR = "output";

  public static final String ANECYARN_INPUTF0RMAT_CLASS = "anecyarn.inputformat.class";

  public static final Class<? extends InputFormat> DEFAULT_ANECYARN_INPUTF0RMAT_CLASS = org.apache.hadoop.mapred.TextInputFormat.class;

  public static final String ANECYARN_OUTPUTFORMAT_CLASS = "anecyarn.outputformat.class";

  public static final Class<? extends OutputFormat> DEFAULT_ANECYARN_OUTPUTF0RMAT_CLASS = TextMultiOutputFormat.class;

  public static final String ANECYARN_INPUTFILE_RENAME = "anecyarn.inputfile.rename";

  public static final Boolean DEFAULT_ANECYARN_INPUTFILE_RENAME = false;

  public static final String ANECYARN_INPUT_STRATEGY = "anecyarn.input.strategy";

  public static final String DEFAULT_ANECYARN_INPUT_STRATEGY = "DOWNLOAD";

  public static final String ANECYARN_OUTPUT_STRATEGY = "anecyarn.output.strategy";

  public static final String DEFAULT_ANECYARN_OUTPUT_STRATEGY = "UPLOAD";

  public static final String ANECYARN_STREAM_EPOCH = "anecyarn.stream.epoch";

  public static final int DEFAULT_ANECYARN_STREAM_EPOCH = 1;

  public static final String ANECYARN_INPUT_STREAM_SHUFFLE = "anecyarn.input.stream.shuffle";

  public static final Boolean DEFAULT_ANECYARN_INPUT_STREAM_SHUFFLE = false;

  public static final String ANECYARN_INPUTFORMAT_CACHESIZE_LIMIT= "anecyarn.inputformat.cachesize.limit";

  public static final int DEFAULT_ANECYARN_INPUTFORMAT_CACHESIZE_LIMIT = 100 * 1024;

  public static final String ANECYARN_INPUTFORMAT_CACHE = "anecyarn.inputformat.cache";

  public static final boolean DEFAULT_ANECYARN_INPUTFORMAT_CACHE = false;

  public static final String ANECYARN_INPUTFORMAT_CACHEFILE_NAME = "anecyarn.inputformat.cachefile.name";

  public static final String DEFAULT_ANECYARN_INPUTFORMAT_CACHEFILE_NAME = "inputformatCache.gz";

  public static final String ANECYARN_INTERREAULST_DIR = "anecyarn.interresult.dir";

  public static final String DEFAULT_ANECYARN_INTERRESULT_DIR = "/interResult_";

  public static final String[] DEFAULT_ANECYARN_APPLICATION_CLASSPATH = {
      "$HADOOP_CONF_DIR",
      "$HADOOP_COMMON_HOME/share/hadoop/common/*",
      "$HADOOP_COMMON_HOME/share/hadoop/common/lib/*",
      "$HADOOP_HDFS_HOME/share/hadoop/hdfs/*",
      "$HADOOP_HDFS_HOME/share/hadoop/hdfs/lib/*",
      "$HADOOP_YARN_HOME/share/hadoop/yarn/*",
      "$HADOOP_YARN_HOME/share/hadoop/yarn/lib/*",
      "$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*",
      "$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*"
  };

  public static final String ANECYARN_TF_BOARD_PATH = "anecyarn.tf.board.path";
  public static final String DEFAULT_ANECYARN_TF_BOARD_PATH = "tensorboard";
  public static final String ANECYARN_TF_BOARD_WORKER_INDEX = "anecyarn.tf.board.worker.index";
  public static final int DEFAULT_ANECYARN_TF_BOARD_WORKER_INDEX = 0;
  public static final String ANECYARN_TF_BOARD_RELOAD_INTERVAL = "anecyarn.tf.board.reload.interval";
  public static final int DEFAULT_ANECYARN_TF_BOARD_RELOAD_INTERVAL = 1;
  public static final String ANECYARN_TF_BOARD_LOG_DIR = "anecyarn.tf.board.log.dir";
  public static final String DEFAULT_ANECYARN_TF_BOARD_LOG_DIR = "eventLog";
  public static final String ANECYARN_TF_BOARD_ENABLE = "anecyarn.tf.board.enable";
  public static final Boolean DEFAULT_ANECYARN_TF_BOARD_ENABLE = true;
  public static final String ANECYARN_TF_BOARD_HISTORY_DIR = "anecyarn.tf.board.history.dir";
  public static final String DEFAULT_ANECYARN_TF_BOARD_HISTORY_DIR = "/tmp/AnecYarn/eventLog";
  public static final String ANECYARN_BOARD_PATH = "anecyarn.board.path";
  public static final String DEFAULT_ANECYARN_BOARD_PATH = "visualDL";
  public static final String ANECYARN_BOARD_MODELPB = "anecyarn.board.modelpb";
  public static final String DEFAULT_ANECYARN_BOARD_MODELPB = "";
  public static final String ANECYARN_BOARD_CACHE_TIMEOUT = "anecyarn.board.cache.timeout";
  public static final int DEFAULT_ANECYARN_BOARD_CACHE_TIMEOUT = 20;
  /**
   * Configuration used in ApplicationMaster
   */
  public static final String ANECYARN_CONTAINER_EXTRA_JAVA_OPTS = "anecyarn.container.extra.java.opts";

  public static final String DEFAULT_ANECYARN_CONTAINER_JAVA_OPTS_EXCEPT_MEMORY = "";

  public static final String ANECYARN_ALLOCATE_INTERVAL = "anecyarn.allocate.interval";

  public static final int DEFAULT_ANECYARN_ALLOCATE_INTERVAL = 1000;

  public static final String ANECYARN_STATUS_UPDATE_INTERVAL = "anecyarn.status.update.interval";

  public static final int DEFAULT_ANECYARN_STATUS_PULL_INTERVAL = 1000;

  public static final String ANECYARN_TASK_TIMEOUT = "anecyarn.task.timeout";

  public static final int DEFAULT_ANECYARN_TASK_TIMEOUT = 5 * 60 * 1000;

  public static final String ANECYARN_LOCALRESOURCE_TIMEOUT = "anecyarn.localresource.timeout";

  public static final int DEFAULT_ANECYARN_LOCALRESOURCE_TIMEOUT = 5 * 60 * 1000;

  public static final String ANECYARN_TASK_TIMEOUT_CHECK_INTERVAL_MS = "anecyarn.task.timeout.check.interval";

  public static final int DEFAULT_ANECYARN_TASK_TIMEOUT_CHECK_INTERVAL_MS = 3 * 1000;

  public static final String ANECYARN_INTERRESULT_UPLOAD_TIMEOUT = "anecyarn.interresult.upload.timeout";

  public static final int DEFAULT_ANECYARN_INTERRESULT_UPLOAD_TIMEOUT = 50 * 60 * 1000;

  public static final String ANECYARN_MESSAGES_LEN_MAX = "anecyarn.messages.len.max";

  public static final int DEFAULT_ANECYARN_MESSAGES_LEN_MAX = 1000;

  public static final String ANECYARN_EXECUTE_NODE_LIMIT = "anecyarn.execute.node.limit";

  public static final int DEFAULT_ANECYARN_EXECUTENODE_LIMIT = 200;

  public static final String ANECYARN_CLEANUP_ENABLE = "anecyarn.cleanup.enable";

  public static final boolean DEFAULT_ANECYARN_CLEANUP_ENABLE = true;

  public static final String ANECYARN_CONTAINER_MAX_FAILURES_RATE = "anecyarn.container.maxFailures.rate";

  public static final double DEFAULT_ANECYARN_CONTAINER_FAILURES_RATE = 0.5;

  public static final String ANECYARN_ENV_MAXLENGTH = "anecyarn.env.maxlength";

  public static final Integer DEFAULT_ANECYARN_ENV_MAXLENGTH = 102400;

  /**
   * Configuration used in Container
   */
  public static final String ANECYARN_DOWNLOAD_FILE_RETRY = "anecyarn.download.file.retry";

  public static final int DEFAULT_ANECYARN_DOWNLOAD_FILE_RETRY = 3;

  public static final String ANECYARN_DOWNLOAD_FILE_THREAD_NUMS = "anecyarn.download.file.thread.nums";

  public static final int DEFAULT_ANECYARN_DOWNLOAD_FILE_THREAD_NUMS = 10;

  public static final String ANECYARN_CONTAINER_HEARTBEAT_INTERVAL = "anecyarn.container.heartbeat.interval";

  public static final int DEFAULT_ANECYARN_CONTAINER_HEARTBEAT_INTERVAL = 10 * 1000;

  public static final String ANECYARN_CONTAINER_HEARTBEAT_RETRY = "anecyarn.container.heartbeat.retry";

  public static final int DEFAULT_ANECYARN_CONTAINER_HEARTBEAT_RETRY = 3;

  public static final String ANECYARN_CONTAINER_UPDATE_APP_STATUS_INTERVAL = "anecyarn.container.update.appstatus.interval";

  public static final int DEFAULT_ANECYARN_CONTAINER_UPDATE_APP_STATUS_INTERVAL = 3 * 1000;

  public static final String ANECYARN_CONTAINER_AUTO_CREATE_OUTPUT_DIR = "anecyarn.container.auto.create.output.dir";

  public static final boolean DEFAULT_ANECYARN_CONTAINER_AUTO_CREATE_OUTPUT_DIR = true;

  public static final String ANECYARN_RESERVE_PORT_BEGIN = "anecyarn.reserve.port.begin";

  public static final int DEFAULT_ANECYARN_RESERVE_PORT_BEGIN = 20000;

  public static final String ANECYARN_RESERVE_PORT_END = "anecyarn.reserve.port.end";

  public static final int DEFAULT_ANECYARN_RESERVE_PORT_END = 30000;

  /**
   * Configuration used in Log Dir
   */
  public static final String ANECYARN_HISTORY_LOG_DIR = "anecyarn.history.log.dir";

  public static final String DEFAULT_ANECYARN_HISTORY_LOG_DIR = "/tmp/AnecYarn/history";

  public static final String ANECYARN_HISTORY_LOG_DELETE_MONITOR_TIME_INTERVAL = "anecyarn.history.log.delete-monitor-time-interval";

  public static final int DEFAULT_ANECYARN_HISTORY_LOG_DELETE_MONITOR_TIME_INTERVAL = 24 * 60 * 60 * 1000;

  public static final String ANECYARN_HISTORY_LOG_MAX_AGE_MS = "anecyarn.history.log.max-age-ms";

  public static final int DEFAULT_ANECYARN_HISTORY_LOG_MAX_AGE_MS = 24 * 60 * 60 * 1000;

  /**
   * Configuration used in Job History
   */
  public static final String ANECYARN_HISTORY_ADDRESS = "anecyarn.history.address";

  public static final String ANECYARN_HISTORY_PORT = "anecyarn.history.port";

  public static final int DEFAULT_ANECYARN_HISTORY_PORT = 10021;

  public static final String DEFAULT_ANECYARN_HISTORY_ADDRESS = "0.0.0.0:" + DEFAULT_ANECYARN_HISTORY_PORT;

  public static final String ANECYARN_HISTORY_WEBAPP_ADDRESS = "anecyarn.history.webapp.address";

  public static final String ANECYARN_HISTORY_WEBAPP_PORT = "anecyarn.history.webapp.port";

  public static final int DEFAULT_ANECYARN_HISTORY_WEBAPP_PORT = 19886;

  public static final String DEFAULT_ANECYARN_HISTORY_WEBAPP_ADDRESS = "0.0.0.0:" + DEFAULT_ANECYARN_HISTORY_WEBAPP_PORT;

  public static final String ANECYARN_HISTORY_WEBAPP_HTTPS_ADDRESS = "anecyarn.history.webapp.https.address";

  public static final String ANECYARN_HISTORY_WEBAPP_HTTPS_PORT = "anecyarn.history.webapp.https.port";

  public static final int DEFAULT_ANECYARN_HISTORY_WEBAPP_HTTPS_PORT = 19885;

  public static final String DEFAULT_ANECYARN_HISTORY_WEBAPP_HTTPS_ADDRESS = "0.0.0.0:" + DEFAULT_ANECYARN_HISTORY_WEBAPP_HTTPS_PORT;

  public static final String ANECYARN_HISTORY_BIND_HOST = "anecyarn.history.bind-host";

  public static final String ANECYARN_HISTORY_CLIENT_THREAD_COUNT = "anecyarn.history.client.thread-count";

  public static final int DEFAULT_ANECYARN_HISTORY_CLIENT_THREAD_COUNT = 10;

  public static final String ANECYARN_HS_RECOVERY_ENABLE = "anecyarn.history.recovery.enable";

  public static final boolean DEFAULT_ANECYARN_HS_RECOVERY_ENABLE = false;

  public static final String ANECYARN_HISTORY_KEYTAB = "anecyarn.history.keytab";

  public static final String ANECYARN_HISTORY_PRINCIPAL = "anecyarn.history.principal";

  /**
   * To enable https in ANECYARN history server
   */
  public static final String ANECYARN_HS_HTTP_POLICY = "anecyarn.history.http.policy";
  public static String DEFAULT_ANECYARN_HS_HTTP_POLICY =
      HttpConfig.Policy.HTTP_ONLY.name();

  /**
   * The kerberos principal to be used for spnego filter for history server
   */
  public static final String ANECYARN_WEBAPP_SPNEGO_USER_NAME_KEY = "anecyarn.webapp.spnego-principal";

  /**
   * The kerberos keytab to be used for spnego filter for history server
   */
  public static final String ANECYARN_WEBAPP_SPNEGO_KEYTAB_FILE_KEY = "anecyarn.webapp.spnego-keytab-file";

  //Delegation token related keys
  public static final String DELEGATION_KEY_UPDATE_INTERVAL_KEY =
      "anecyarn.cluster.delegation.key.update-interval";
  public static final long DELEGATION_KEY_UPDATE_INTERVAL_DEFAULT =
      24 * 60 * 60 * 1000; // 1 day
  public static final String DELEGATION_TOKEN_RENEW_INTERVAL_KEY =
      "anecyarn.cluster.delegation.token.renew-interval";
  public static final long DELEGATION_TOKEN_RENEW_INTERVAL_DEFAULT =
      24 * 60 * 60 * 1000;  // 1 day
  public static final String DELEGATION_TOKEN_MAX_LIFETIME_KEY =
      "anecyarn.cluster.delegation.token.max-lifetime";
  public static final long DELEGATION_TOKEN_MAX_LIFETIME_DEFAULT =
      24 * 60 * 60 * 1000; // 7 days
}
