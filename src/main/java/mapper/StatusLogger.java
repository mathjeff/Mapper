package mapper;

import java.util.List;

// A StatusLogger is used for logging status messages
// A StatusLogger might choose not to log all messages if there are too many status updates consecutively
public class StatusLogger {
  public StatusLogger(Logger logger, long startMillis) {
    this.logger = logger;
    this.lastLoggedAt = 0;
    this.startMillis = startMillis;
  }

  // If no message was received recently or if this message is important, then this method will log it
  public void log(String message, boolean important) {
    if (this.logger.getEnabled()) {
      long now = System.currentTimeMillis();
      if (now - lastLoggedAt > 1000 || important) {
        this.lastLoggedAt = now;
        double elapsedSeconds = (now - this.startMillis) / 1000.0;
          this.logger.log(message + " at " + elapsedSeconds + "s");
      }
    }
  }

  public Logger getLogger() {
    return this.logger;
  }

  Logger logger;
  long lastLoggedAt;
  long startMillis;
}
