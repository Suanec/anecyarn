package anec.ml.yarn.anecyarn.common.exceptions;

public class RequestOverLimitException extends AnecYarnExecException {

  private static final long serialVersionUID = 1L;

  public RequestOverLimitException(String message) {
    super(message);
  }
}
