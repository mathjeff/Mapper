package mapper;

import java.util.List;

public class StringWriter implements TextWriter {
  public StringWriter() {
  }

  public void write(String message) {
    synchronized(this) {
      this.builder.append(message);
      this.builder.append("\n");
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

  public String getText() {
    return this.builder.toString();
  }

  private StringBuilder builder = new StringBuilder();
}
