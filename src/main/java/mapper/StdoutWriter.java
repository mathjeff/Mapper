package mapper;

public class StdoutWriter implements TextWriter {
  public StdoutWriter() {
  }

  public void write(String message) {
    System.out.println(message);
  }

  public void flush() {
  }
}
