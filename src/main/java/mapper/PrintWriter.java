package mapper;

public class PrintWriter implements TextWriter {
  public PrintWriter() {
  }

  public void write(String message) {
    System.out.println(message);
  }

  public void flush() {
  }
}
