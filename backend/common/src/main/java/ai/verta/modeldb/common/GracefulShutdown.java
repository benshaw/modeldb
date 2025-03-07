package ai.verta.modeldb.common;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.catalina.connector.Connector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.boot.web.embedded.tomcat.TomcatConnectorCustomizer;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;

public class GracefulShutdown
    implements TomcatConnectorCustomizer, ApplicationListener<ContextClosedEvent> {

  private Connector connector;
  private long shutdownTimeout;
  private static final Logger LOGGER = LogManager.getLogger(GracefulShutdown.class);

  public GracefulShutdown(Long shutdownTimeout) {
    this.shutdownTimeout = shutdownTimeout;
  }

  @Override
  public void customize(Connector connector) {
    this.connector = connector;
  }

  @Override
  public void onApplicationEvent(ContextClosedEvent event) {
    this.connector.pause();
    var executor = this.connector.getProtocolHandler().getExecutor();
    if (executor instanceof ThreadPoolExecutor) {
      try {
        var threadPoolExecutor = (ThreadPoolExecutor) executor;
        int nfsActiveRequestCount = threadPoolExecutor.getActiveCount();

        while (nfsActiveRequestCount > 0) {
          nfsActiveRequestCount = threadPoolExecutor.getActiveCount();
          LOGGER.info("NFS Active Request Count in while: {}", nfsActiveRequestCount);
          waitForSomeTime();
        }

        threadPoolExecutor.shutdown();
        if (!threadPoolExecutor.awaitTermination(shutdownTimeout, TimeUnit.SECONDS)) {
          LOGGER.info(
              "NFS Server thread pool did not shut down gracefully within "
                  + "{} seconds. Proceeding with forceful shutdown",
              shutdownTimeout);

          threadPoolExecutor.shutdownNow();

          if (!threadPoolExecutor.awaitTermination(shutdownTimeout, TimeUnit.SECONDS)) {
            LOGGER.info("NFS Server thread pool did not terminate");
          }
        } else {
          LOGGER.info("*** NFS Server Shutdown ***");
        }
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private void waitForSomeTime() {
    try {
      Thread.sleep(1000); // wait for 1s
    } catch (InterruptedException e) {
      LOGGER.error(e.getMessage(), e);
      // Restore interrupted state...
      Thread.currentThread().interrupt();
    }
  }
}
