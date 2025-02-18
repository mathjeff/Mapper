package mapper;

public class StderrWriter implements TextWriter {
  public StderrWriter() {
  }

  public void write(String message) {
    System.err.println(message);
  }

  public void flush() {
  }
}
