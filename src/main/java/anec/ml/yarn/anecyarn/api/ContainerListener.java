package anec.ml.yarn.anecyarn.api;

import anec.ml.yarn.anecyarn.container.AnecYarnContainerId;

public interface ContainerListener {

  void registerContainer(AnecYarnContainerId xlearningContainerId, String role);

  boolean isAllPsContainersFinished();

  boolean isTrainCompleted();

  boolean isAllWorkerContainersSucceeded();

  int interResultCompletedNum(Long lastInnerModel);
}
