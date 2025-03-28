package mapper;

import java.util.List;

public class StderrWriter implements TextWriter {
  public StderrWriter() {
  }

  public void write(String message) {
    synchronized(this) {
      System.err.println(message);
    }
  }

  public void write(List<String> messages) {
    synchronized(this) {
      for (String message: messages) {
        this.write(message);
      }
    }
  }

  public void flush() {
  }
}
