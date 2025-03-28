package mapper;

import java.util.ArrayList;
import java.util.List;

public class BufferedWriter implements TextWriter {
  public BufferedWriter(TextWriter writer, String title, int approximateMaxNumMessages) {
    this.title = title;
    this.writer = writer;
    this.approximateMaxNumMessages = approximateMaxNumMessages;
  }

  public void write(String message) {
    this.ensureTitle();
    this.components.add(message);
    this.flushIfManyMessages();
  }

  public void write(List<String> messages) {
    this.ensureTitle();
    this.components.addAll(messages);
    this.flushIfManyMessages();
  }

  public void flush() {
    this.writer.write(this.components);
    this.components.clear();
  }

  private void ensureTitle() {
    if (this.components.size() < 1)
      this.components.add(this.title);
  }

  private void flushIfManyMessages() {
    if (this.components.size() >= this.approximateMaxNumMessages)
      this.flush();
  }

  List<String> components = new ArrayList<String>();
  TextWriter writer;
  String title;
  int approximateMaxNumMessages;
}
