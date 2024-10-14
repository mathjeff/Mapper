package mapper;

public class PrintWriter implements TextWriter {
  public PrintWriter() {
  }

  public void write(String message) {
    System.err.println(message);
  }

  public void flush() {
  }
}
