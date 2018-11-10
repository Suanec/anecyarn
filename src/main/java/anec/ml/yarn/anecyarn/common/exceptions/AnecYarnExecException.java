
package anec.ml.yarn.anecyarn.common.exceptions;

public class AnecYarnExecException extends RuntimeException {

  private static final long serialVersionUID = 1L;


  public AnecYarnExecException() {
  }

  public AnecYarnExecException(String message) {
    super(message);
  }

  public AnecYarnExecException(String message, Throwable cause) {
    super(message, cause);
  }

  public AnecYarnExecException(Throwable cause) {
    super(cause);
  }

  public AnecYarnExecException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
