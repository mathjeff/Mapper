package mapper;

import java.util.ArrayList;
import java.util.List;

public class BufferedWriter implements TextWriter {
  public BufferedWriter() {
  }

  public void write(String message) {
    this.components.add(message);
  }

  public void flush() {
    for (String component : components) {
      System.err.println(component);
    }
    this.components.clear();
  }

  List<String> components = new ArrayList<String>();
}
