package mapper;

import java.util.List;

public class StdoutWriter implements TextWriter {
  public StdoutWriter() {
  }

  public void write(String message) {
    synchronized(this) {
      System.out.println(message);
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
