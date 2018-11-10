package anec.ml.yarn.anecyarn.webapp;

import anec.ml.yarn.anecyarn.api.ApplicationContext;

import com.google.inject.Inject;
import com.google.inject.servlet.RequestScoped;

@RequestScoped
public class App {
  final ApplicationContext context;

  @Inject
  App(ApplicationContext context) {
    this.context = context;
  }
}
